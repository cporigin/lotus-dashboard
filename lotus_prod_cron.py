if __name__ == "__main__":
    from lotus_cron import LotosDashboardCron

    LotosDashboardCron("config-prod.json").fetch_lead_insight()
