from datetime import datetime
import time


if __name__ == "__main__":
    from lotus_cron import LotosDashboardCron

    cron = LotosDashboardCron("config-stg.json")
    while True:
        cron.fetch()
        print("Sleeping for 60 seconds", datetime.now())
        time.sleep(60)
