import numpy as np, pandas as pd, json
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from lotus_dashboard.database import Base
import lotus_dashboard.models as models
from lotus_dashboard.models.lead_insight import LeadInsight
from lotus_dashboard.models.user_performance import UserPerformance


class LotosDashboardCron:

    def __init__(self, path: str) -> None:
        self.local_engine = None
        self.server_engine = None
        self.config = self.load_config(path)
        self.init_db()

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
            host = config_json["MYSQL_HOST"]
            if "MYSQL_PORT" in config_json:
                host = f"{host}:{config_json['MYSQL_PORT']}"
            SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{config_json['MYSQL_USER']}:{config_json['MYSQL_PASSWORD']}@{host}/{config_json['MYSQL_DB']}{config_json['MYSQL_PARAMS']}"
            self.local_engine = create_engine(SQLALCHEMY_DATABASE_URI)
        return self.local_engine

    def get_server_engine(self):
        if self.server_engine is None:
            config_json = self.config["DASHBOARD_DB"]
            host = config_json["MYSQL_HOST"]
            if "MYSQL_PORT" in config_json:
                host = f"{host}:{config_json['MYSQL_PORT']}"
            SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{config_json['MYSQL_USER']}:{config_json['MYSQL_PASSWORD']}@{host}/{config_json['MYSQL_DB']}{config_json['MYSQL_PARAMS']}"
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
        df.replace({np.nan: None, pd.NaT: None, "NaT": None, "NaN": None}, inplace=True)
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
        df.replace({np.nan: None, pd.NaT: None, "NaT": None, "NaN": None}, inplace=True)
        df.to_sql(
            model.__tablename__,
            con=engine.connect(),
            if_exists="append",
            index=False,
            method="multi",
        )

    def fetch(self):
        data = self.fetch_data()
        df_lead_insight = self.fetch_lead_insight(data)
        df_user_performance = self.fetch_user_performance(data)
        self.truncate_db()
        self.dataframe_to_db(LeadInsight, df_lead_insight)
        self.dataframe_to_db(UserPerformance, df_user_performance)

    def fetch_data(self):
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
        df_user = pd.read_sql("select * from user", conn)
        df_user_access = pd.read_sql("select * from user_access", conn)
        return (
            df_lead,
            df_deal,
            df_mall,
            df_area,
            df_area_deal,
            df_group,
            df_deal_task,
            df_deal_comment,
            df_user,
            df_user_access,
        )

    def fetch_lead_insight(self, data: tuple):
        (
            df_lead,
            df_deal,
            df_mall,
            df_area,
            df_area_deal,
            df_group,
            df_deal_task,
            df_deal_comment,
            df_user,
            df_user_access,
        ) = data

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
        return df_lead_insight

    def fetch_user_performance(self, data: tuple):
        (
            df_lead,
            df_deal,
            df_mall,
            df_area,
            df_area_deal,
            df_group,
            df_deal_task,
            df_deal_comment,
            df_user,
            df_user_access,
        ) = data

        df_kpi = pd.merge(
            df_lead.rename(
                columns={
                    "id": "lead_id",
                    "created_at": "lead_created_at",
                    "updated_at": "lead_updated_at",
                }
            )[["lead_id", "lead_created_at"]],
            df_deal.rename(
                columns={
                    "id": "deal_id",
                    "code": "deal_code",
                    "created_at": "deal_created_at",
                    "updated_at": "deal_updated_at",
                }
            ).drop(columns=["user_id"]),
            on="lead_id",
            how="left",
        )
        df_kpi = pd.merge(
            df_kpi,
            df_deal_task.rename(
                columns={
                    "group_id": "assigned_group",
                    "created_at": "task_created_at",
                    "due_date": "task_due_date",
                    "id": "deal_task_id",
                    "status": "deal_task_status",
                    "updated_at": "task_updated_at",
                }
            ),
            on="deal_id",
            how="left",
        )
        df_kpi = pd.merge(
            df_kpi,
            df_deal_comment.rename(
                columns={
                    "id": "deal_comment_id",
                    "created_at": "comment_created_at",
                    "user_id": "deal_comment_user_id",
                }
            ),
            on=["deal_id", "deal_task_id"],
            how="left",
        ).drop(columns=["loi_reference_x", "loi_reference_y"])
        df_kpi.loc[
            df_kpi.query("status=='win' or status =='lose'").index,
            "time_close_deal_task",
        ] = (
            df_kpi.query("status=='win' or status =='lose'")["comment_created_at"]
            - df_kpi.query("status=='win' or status =='lose'")["task_created_at"]
        )
        df_kpi.loc[
            df_kpi.query("time_close_deal_task.notna()").index, "time_close_deal_task"
        ] = [
            i.total_seconds() / 3600
            for i in df_kpi.loc[
                df_kpi.query("time_close_deal_task.notna()").index,
                "time_close_deal_task",
            ]
        ]

        df_kpi_time_first_activity = df_kpi.copy().drop_duplicates(
            ["lead_id", "deal_id", "deal_task_id"], keep="first"
        )
        df_kpi_time_first_activity = df_kpi_time_first_activity.query(
            "comment_created_at.notna() or task_status== 'transfer'"
        )
        df_kpi_time_first_activity.loc[
            df_kpi_time_first_activity.query("comment_created_at.notna()").index,
            "time_first_activity",
        ] = (
            df_kpi_time_first_activity.query("comment_created_at.notna()")[
                "comment_created_at"
            ]
            - df_kpi_time_first_activity.query("comment_created_at.notna()")[
                "task_created_at"
            ]
        )
        df_kpi_time_first_activity.loc[
            df_kpi_time_first_activity.query("comment_created_at.isna()").index,
            "time_first_activity",
        ] = (
            df_kpi_time_first_activity.query("comment_created_at.isna()")[
                "task_updated_at"
            ]
            - df_kpi_time_first_activity.query("comment_created_at.isna()")[
                "task_created_at"
            ]
        )
        df_kpi_time_first_activity["time_first_activity"] = [
            i.total_seconds() / 3600
            for i in df_kpi_time_first_activity["time_first_activity"]
        ]

        df_kpi_time_first_contacted = (
            df_kpi.copy()
            .query("status=='contacted'")
            .drop_duplicates(["lead_id", "deal_id", "deal_task_id"], keep="first")
        )
        df_kpi_time_first_contacted["time_first_contacted"] = (
            df_kpi_time_first_contacted["comment_created_at"]
            - df_kpi_time_first_contacted["task_created_at"]
        )
        df_kpi_time_first_contacted["time_first_contacted"] = [
            i.total_seconds() / 3600
            for i in df_kpi_time_first_contacted["time_first_contacted"]
        ]

        # df_kpi_time_doing_task = df_kpi.copy().drop_duplicates(['lead_id','deal_id','task_id'],keep='last')
        df_kpi_time_doing_task = df_kpi.copy()
        df_kpi_time_doing_task.loc[
            df_kpi_time_doing_task.query("status=='win' or status == 'lose'").index,
            "time_doing_task",
        ] = (
            df_kpi_time_doing_task.loc[
                df_kpi_time_doing_task.query("status=='win' or status == 'lose'").index,
                "comment_created_at",
            ]
            - df_kpi_time_doing_task.loc[
                df_kpi_time_doing_task.query("status=='win' or status == 'lose'").index,
                "task_created_at",
            ]
        )
        df_kpi_time_doing_task_get_last = df_kpi_time_doing_task.copy().drop_duplicates(
            ["lead_id", "deal_id", "task_id"], keep="last"
        )
        df_kpi_time_doing_task_get_last.loc[
            df_kpi_time_doing_task_get_last.query(
                "task_status=='transfer' or task_status=='expired'"
            ).index,
            "time_doing_task",
        ] = (
            df_kpi_time_doing_task_get_last.query(
                "task_status=='transfer' or task_status=='expired'"
            )["task_updated_at"]
            - df_kpi_time_doing_task_get_last.query(
                "task_status=='transfer' or task_status=='expired'"
            )["task_created_at"]
        )
        df_kpi_time_doing_task.loc[
            df_kpi_time_doing_task_get_last.index, "time_doing_task"
        ] = df_kpi_time_doing_task_get_last["time_doing_task"]
        df_kpi_time_doing_task["time_doing_task"] = df_kpi_time_doing_task[
            "time_doing_task"
        ].apply(lambda x: x.total_seconds() / 3600 if pd.notnull(x) else np.nan)

        df_kpi.loc[df_kpi_time_first_activity.index, "time_first_activity"] = (
            df_kpi_time_first_activity["time_first_activity"]
        )
        df_kpi.loc[df_kpi_time_first_contacted.index, "time_first_contacted"] = (
            df_kpi_time_first_contacted["time_first_contacted"]
        )
        df_kpi.loc[df_kpi_time_doing_task.index, "time_doing_task"] = (
            df_kpi_time_doing_task["time_doing_task"]
        )

        list_recently_task_on_deal = df_kpi.drop_duplicates(
            ["lead_id", "deal_id"], keep="last"
        )["task_id"]
        index_recently_task = df_kpi.query(
            "task_id in @list_recently_task_on_deal"
        ).index
        df_kpi["is_recent_task_on_deal"] = False
        df_kpi.loc[index_recently_task, "is_recent_task_on_deal"] = True

        df_user_respond = pd.merge(
            df_user.rename(columns={"id": "user_id"}),
            df_user_access.rename(columns={"id": "user_access_id"}),
            on="user_id",
            how="left",
        )

        df_user_respond = pd.merge(
            df_group.rename(columns={"id": "group_id", "name": "group_name"}),
            df_user_respond,
            on="group_id",
            how="left",
        )
        index_hq_mall = df_user_respond.query("role=='hq_manager' and type=='mall'")
        df_user_respond = df_user_respond.drop(index_hq_mall.index)

        index_area_mall = df_user_respond.query("role=='area_manager' and type=='mall'")
        df_user_respond = df_user_respond.drop(index_area_mall.index)

        index_region_mall = df_user_respond.query(
            "role=='region_manager' and type=='mall'"
        )
        df_user_respond = df_user_respond.drop(index_region_mall.index)

        df_user_respond["first_last"] = (
            df_user_respond["first_name"] + " " + df_user_respond["last_name"]
        )

        df_user_respond = df_user_respond.groupby(
            ["group_id", "group_name", "code", "type"], as_index=False
        ).agg(
            {
                "username": list,
                "first_last": list,
                "last_active_date": list,
                "role": list,
                "user_id": list,
            }
        )

        ##############################################################################

        df_kpi = pd.merge(
            df_kpi,
            df_user_respond.rename(columns={"group_id": "assigned_group"}),
            on=["assigned_group"],
            how="left",
        )
        df_kpi_check = df_kpi.copy()

        df_kpi = pd.merge(
            df_kpi,
            df_mall[["code", "region", "area_code", "province"]],
            how="left",
            on="code",
        )

        df_kpi

        ##############################################################################

        df_kpi_performance_user = df_kpi.copy()
        recently_task_on_deal = df_kpi_performance_user.drop_duplicates(
            ["lead_id", "deal_id"], keep="last"
        )["deal_task_id"].unique()

        df_kpi_performance_user = df_kpi_performance_user.query(
            "deal_task_id in @recently_task_on_deal"
        )
        # df_kpi_performance_user = pd.merge(df_kpi_performance_user,df_user.rename(columns={"id":"deal_comment_user_id","last_active_date":"user_last_active_date"}),how="left",on="deal_comment_user_id")

        df_kpi_performance_user_exploded = df_kpi_performance_user.query(
            "(deal_task_status == 'new' and task_status == 'active') or (deal_comment_user_id.isna())"
        )

        df_kpi_performance_user = df_kpi_performance_user.drop(
            df_kpi_performance_user_exploded.index
        )
        df_kpi_performance_user_exploded = df_kpi_performance_user_exploded.explode(
            [
                "user_id",
                "last_active_date",
            ]
        )

        df_kpi_performance_user_exploded["deal_comment_user_id"] = (
            df_kpi_performance_user_exploded["user_id"]
        )
        df_kpi_performance_user = pd.concat(
            [df_kpi_performance_user, df_kpi_performance_user_exploded]
        ).sort_values(["lead_id", "deal_id", "deal_task_id"])

        # df_kpi_check= df_kpi_performance_user.copy()

        df_kpi_performance_user = pd.merge(
            df_kpi_performance_user,
            df_user.rename(
                columns={
                    "id": "deal_comment_user_id",
                    "last_active_date": "user_comment_last_active_date",
                    "created_at": "user_comment_created_at",
                }
            )[
                [
                    "deal_comment_user_id",
                    "user_comment_last_active_date",
                    "first_name",
                    "last_name",
                    "user_comment_created_at",
                ]
            ],
            how="left",
            on="deal_comment_user_id",
        )
        df_kpi_performance_user["user_comment_first_last"] = (
            df_kpi_performance_user["first_name"]
            + " "
            + df_kpi_performance_user["last_name"]
        )
        # df_kpi_performance_user = df_kpi_performance_user.query("user_created_at<=task_created_at")
        columns_drop = [
            "username",
            "first_last",
            "last_active_date",
            "role",
            "user_id",
            "state_flows",
        ]
        df_kpi_performance_user = df_kpi_performance_user.drop(columns=columns_drop)
        return df_kpi_performance_user
