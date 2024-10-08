import numpy as np, pandas as pd, json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from lotus_dashboard.database import Base
import lotus_dashboard.models as models


class LotosDashboardCron:

    def __init__(self, path: str) -> None:
        self.local_engine = None
        self.server_engine = None
        self.config = self.load_config(path)

    def init_db(self):
        local_engine = self.get_local_engine()
        Base.metadata.create_all(bind=local_engine)

    def get_session(self):
        local_engine = self.get_local_engine()
        session = sessionmaker(autocommit=False, autoflush=False, bind=local_engine)
        return session()

    def load_config(self, path: str):
        config = {}
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
        return config

    def get_local_engine(self):
        if self.local_engine is None:
            config_json = self.config
            SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{config_json['MYSQL_USER']}:{config_json['MYSQL_PASSWORD']}@{config_json['MYSQL_HOST']}:{config_json['MYSQL_PORT']}/{config_json['MYSQL_DB']}{config_json['MYSQL_PARAMS']}"
            self.local_engine = create_engine(SQLALCHEMY_DATABASE_URI)
        return self.local_engine

    def get_server_engine(self):
        if self.server_engine is None:
            config_json = self.config["DASHBOARD_DB"]
            SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{config_json['MYSQL_USER']}:{config_json['MYSQL_PASSWORD']}@{config_json['MYSQL_HOST']}/{config_json['MYSQL_DB']}{config_json['MYSQL_PARAMS']}"
            self.server_engine = create_engine(SQLALCHEMY_DATABASE_URI)
        return self.server_engine

    def truncate_db(self):
        sess = self.get_session()
        sess.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        for table in Base.metadata.tables:
            sess.execute(text(f"TRUNCATE TABLE `{table}`"))
        sess.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
        sess.commit()
        sess.close()

    def import_csv_to_db(self, model: Base, csv_file: str):
        engine = self.get_local_engine()
        df = pd.read_csv(csv_file)
        df.where(df.notnull(), None)
        df.replace({np.nan: None, pd.NaT: None, "NaT": None}, inplace=True)
        self.truncate_db()
        df.to_sql(
            model.__tablename__,
            con=engine.connect(),
            if_exists="append",
            index=False,
            method="multi",
        )

    def dataframe_to_db(self, model: Base, df: pd.DataFrame):
        engine = self.get_local_engine()
        df.where(df.notnull(), None)
        df.replace({np.nan: None, pd.NaT: None, "NaT": None}, inplace=True)
        self.truncate_db()
        df.to_sql(
            model.__tablename__,
            con=engine.connect(),
            if_exists="append",
            index=False,
            method="multi",
        )

    def fetch_lead_insight(self):
        engine = self.get_server_engine()
        conn = engine.connect()
        df_lead = pd.read_sql("select * from `lead`", conn)
        df_deal = pd.read_sql("select * from deal", conn)
        df_mall = pd.read_sql("select * from mall", conn)
        df_area = pd.read_sql("select * from area", conn)
        df_area_deal = pd.read_sql("select * from area_deal", conn)
        df_group = pd.read_sql("select * from `group`", conn)
        df_deal_task = pd.read_sql("select * from deal_task", conn)
        df_deal_comment = pd.read_sql("select * from deal_comment", conn)

        df_lead_insight = pd.merge(
            df_lead.rename(columns={"id": "lead_id"}), df_deal, how="left", on="lead_id"
        )
        df_lead_insight = (
            df_lead_insight[
                [
                    "lead_id",
                    "id",
                    "state",
                    "user_id_x",
                    "category",
                    "store_format",
                    "source",
                    "brand_type",
                    "rent_type",
                    "size_range",
                    "created_at_x",
                    "created_at_y",
                ]
            ]
            .rename(
                columns={
                    "created_at_x": "lead_created_at",
                    "created_at_y": "deal_created_at",
                    "id": "deal_id",
                }
            )
            .rename(columns={"user_id_x": "lead_sender"})
        )
        df_lead_insight = pd.merge(
            df_lead_insight,
            pd.merge(
                df_area.rename(columns={"id": "area_id"}),
                df_area_deal,
                how="left",
                on="area_id",
            )[["lead_id", "deal_id", "type", "mall_id", "province"]],
            how="left",
            on=["lead_id", "deal_id"],
        ).rename(columns={"province": "area_province"})

        df_lead_insight = pd.merge(
            df_lead_insight,
            df_mall.rename(
                columns={
                    "id": "mall_id",
                    "type": "mall_type",
                    "region": "mall_region",
                    "district": "mall_district",
                }
            )[
                [
                    "mall_id",
                    "name",
                    "province",
                    "mall_type",
                    "mall_region",
                    "mall_district",
                ]
            ],
            how="left",
            on="mall_id",
        ).rename(columns={"name": "mall_name", "province": "mall_province"})

        df_lead_insight["province"] = df_lead_insight["area_province"].fillna(
            ""
        ) + df_lead_insight["mall_province"].fillna("")
        ################################### Causion ##################################
        df_lead_insight = df_lead_insight.drop_duplicates(
            ["lead_id", "deal_id"], keep="last"
        )
        ################################### </>Causion ##################################

        df_lead_insight["lead_created_day_of_week"] = [
            i.date().strftime("%A") for i in df_lead_insight["lead_created_at"]
        ]

        df_lead_insight["lead_sender"] = df_lead_insight["lead_sender"].fillna("tenant")
        df_lead_insight.loc[
            df_lead_insight.query("lead_sender!='tenant'").index, "lead_sender"
        ] = "employee"

        df_lead_insight = pd.merge(
            df_lead_insight,
            df_deal_task.rename(
                columns={
                    "id": "deal_task_id",
                    "due_date": "deal_task_due_date",
                    "updated_at": "deal_task_updated_at",
                }
            )[
                [
                    "deal_task_id",
                    "deal_id",
                    "task_id",
                    "status",
                    "task_status",
                    "deal_task_due_date",
                    "deal_task_updated_at",
                ]
            ],
            on="deal_id",
            how="left",
        )

        mapping_format = {
            "hypermarket": "Hypermarket",
            "all": "All",
            "mini_supermarket": "Mini_Supermarket",
            "supermarket": "Supermarket",
            "cpfm": "CPFM",
        }
        df_lead_insight = df_lead_insight.replace(mapping_format)
        df_lead_insight = df_lead_insight.replace(
            {"employee": "Employee", "tenant": "Tenant"}
        )

        df_lead_insight = pd.merge(
            df_lead_insight,
            df_deal_comment.rename(
                columns={
                    "created_at": "comment_created_at",
                    "status": "comment_status",
                    "id": "comment_id",
                }
            ),
            on=["deal_id", "deal_task_id"],
            how="left",
        )

        df_deal_closed = df_lead_insight.copy().query(
            "comment_status=='win' or comment_status =='lose'"
        )
        df_deal_closed["deal_time_used"] = (
            df_deal_closed["comment_created_at"] - df_deal_closed["deal_created_at"]
        )
        df_deal_closed["deal_time_used"] = [
            i.total_seconds() / 3600 for i in df_deal_closed["deal_time_used"]
        ]

        df_first_activity = df_lead_insight.copy().drop_duplicates(
            "deal_id", keep="first"
        )
        df_first_activity = df_first_activity.query(
            "comment_created_at.notna() or task_status== 'transfer'"
        )
        df_first_activity.loc[
            df_first_activity.query("comment_created_at.notna()").index,
            "time_first_activity",
        ] = (
            df_first_activity.query("comment_created_at.notna()")["comment_created_at"]
            - df_first_activity.query("comment_created_at.notna()")["deal_created_at"]
        )
        df_first_activity.loc[
            df_first_activity.query("comment_created_at.isna()").index,
            "time_first_activity",
        ] = (
            df_first_activity.query("comment_created_at.isna()")["deal_task_updated_at"]
            - df_first_activity.query("comment_created_at.isna()")["deal_created_at"]
        )
        df_first_activity["time_first_activity"] = [
            i.total_seconds() / 3600 for i in df_first_activity["time_first_activity"]
        ]

        df_first_contacted_time = (
            df_lead_insight.copy()
            .query("comment_status == 'contacted'")
            .drop_duplicates(["lead_id", "deal_id"])[
                [
                    "lead_id",
                    "deal_id",
                    "comment_status",
                    "comment_created_at",
                    "deal_created_at",
                ]
            ]
        )
        df_first_contacted_time["first_contacted_time_used"] = (
            df_first_contacted_time["comment_created_at"]
            - df_first_contacted_time["deal_created_at"]
        )
        df_first_contacted_time["first_contacted_time_used"] = [
            i.total_seconds() / 3600
            for i in df_first_contacted_time["first_contacted_time_used"]
        ]

        df_lead_insight = df_lead_insight.drop_duplicates("deal_id", keep="last")
        df_lead_insight = pd.merge(
            df_lead_insight,
            df_first_activity[["deal_id", "time_first_activity"]],
            how="left",
            on="deal_id",
        )
        df_lead_insight = pd.merge(
            df_lead_insight,
            df_first_contacted_time[["deal_id", "first_contacted_time_used"]],
            how="left",
            on="deal_id",
        )

        df_lead_insight = pd.merge(
            df_lead_insight,
            df_deal_closed[["deal_id", "deal_time_used"]],
            how="left",
            on="deal_id",
        )
        self.dataframe_to_db(models.LeadInsight, df_lead_insight)
