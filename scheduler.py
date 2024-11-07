from crontab import CronTab

def setup_cron_jobs():
    cron = CronTab(user=True)

    # Define the command to run the crawlers and additional jobs
    crawlers_and_jobs_command = 'python /script.py'
    crawlers_and_jobs_schedule = '0 0 * * *'  # Every day at 00:00 AM

    # Add cron job for running the crawlers and additional jobs
    crawlers_and_jobs_job = cron.new(command=crawlers_and_jobs_command, comment='Run crawlers and additional jobs')
    crawlers_and_jobs_job.setall(crawlers_and_jobs_schedule)

    # Write the cron tab
    cron.write("/config.txt")

    print("Cron jobs set up successfully.")

# Function to run the scheduler
def run_scheduler():
    setup_cron_jobs()

if __name__ == "__main__":
    run_scheduler()
