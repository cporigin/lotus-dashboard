if __name__ == "__main__":
    from lotus_cron import LotosDashboardCron

    LotosDashboardCron("config-stg.json").fetch_lead_insight()
