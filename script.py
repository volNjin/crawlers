from multiprocessing import Process, freeze_support
import subprocess


# Function to run the Selenium crawler
def run_selenium_crawler():
    # Replace '/path/to/your/selenium_crawler.py' with the actual path to your Selenium crawler script
    selenium_crawler_path = "agoda\selenium_python\sendRq.py"

    # Run the Selenium crawler script
    subprocess.run(["python", selenium_crawler_path], check=True)


# Function to run the first Scrapy crawler
def run_scrapy_crawler1():
    # Replace 'spider1' with the name of your first Scrapy spider
    scrapy_crawler1_command = "scrapy crawl booking"

    # Run the first Scrapy crawler
    subprocess.run(
        scrapy_crawler1_command, shell=True, check=True, cwd="booking/crawlers"
    )


# Function to run the second Scrapy crawler
def run_scrapy_crawler2():
    # Replace 'spider2' with the name of your second Scrapy spider
    scrapy_crawler2_command = "scrapy crawl crawler"

    # Run the second Scrapy crawler
    subprocess.run(
        scrapy_crawler2_command, shell=True, check=True, cwd="traveloka/traveloka"
    )


# Function to run additional jobs
def run_additional_jobs():
    # Replace these lines with the additional jobs you want to run
    print("Additional jobs running...")
    subprocess.run(["python", "sentiment_analyzer.py"], check=True)
    subprocess.run(["python", "combine_db.py"], check=True)


# Function to run the crawlers and additional jobs
def run_crawlers_and_additional_jobs():
    # Start processes for each crawler
    selenium_process = Process(target=run_selenium_crawler)
    scrapy_process1 = Process(target=run_scrapy_crawler1)
    scrapy_process2 = Process(target=run_scrapy_crawler2)
    if __name__ == '__main__':
        # Add freeze_support() for Windows
        freeze_support()
        selenium_process.start()
        scrapy_process1.start()
        scrapy_process2.start()

        # Wait for all crawlers to complete
        selenium_process.join()
        scrapy_process1.join()
        scrapy_process2.join()

        # Run additional jobs after all crawlers are completed
        run_additional_jobs()


run_crawlers_and_additional_jobs()
