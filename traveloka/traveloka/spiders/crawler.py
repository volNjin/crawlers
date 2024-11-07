import scrapy
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
)
from datetime import datetime, timedelta
from urllib.parse import urlencode
import time
import json
from selenium.webdriver.chrome.options import Options
from pymongo import MongoClient
from traveloka.items import HomestayItem
from traveloka.items import RoomItem
from scrapy_splash import SplashRequest
import re


class CrawlerSpider(scrapy.Spider):
    name = "crawler"
    allowed_domains = ["traveloka.com"]
    start_urls = ["https://traveloka.com"]

    client = MongoClient("mongodb://localhost:27017")
    database_name = "traveloka_homestays"
    db = client[database_name]

    db["homes"].drop()
    db["rooms"].drop()
    db["reviews"].drop()
    db.create_collection("homes")
    home_collection = db["homes"]

    db.create_collection("rooms")
    room_collection = db["rooms"]

    db.create_collection("reviews")
    review_collection = db["reviews"]

    def start_requests(self):
        current_date = datetime.now()
        start_date = current_date + timedelta(days=1)
        end_date = current_date + timedelta(days=2)
        with open("cities.json", "r", encoding="utf-8") as file:
            data = json.load(file)

        cities = data["cities"]
        base_url = "https://www.traveloka.com/vi-vn/hotel/search?"
        search_params = {
            "spec": start_date.strftime("%d-%m-%Y")
            + "."
            + end_date.strftime("%d-%m-%Y")
        }
        url1 = f"{base_url}{urlencode(search_params)}"
        options = Options()
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--disable-blink-features=AutomationControlled")
        driver = webdriver.Chrome(options=options)
        driver.get(url1)
        driver.maximize_window()
        try:
            modal_close_button = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located(
                    (
                        By.CSS_SELECTOR,
                        "div.css-1dbjc4n.r-1loqt21.r-1472mwg.r-u8s1d.r-e1k2in.r-19lq7b1.r-1otgn73.r-lrsllp.r-mhe3cw",
                    )
                )
            )
            modal_close_button.click()
        except:
            TimeoutError
        driver2 = webdriver.Chrome(options=options)
        driver2.maximize_window()
        for city in cities:
            textInput = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located(
                    (By.CSS_SELECTOR, 'input[data-testid="autocomplete-field"]')
                )
            )
            textInput.send_keys(city["vn"])

            time.sleep(2)
            addresses = driver.find_elements(
                By.CSS_SELECTOR, 'div[data-testid="dropdown-menu-item"]'
            )
            for address in addresses:
                if city["vn"] in address.text or city["en"] in address.text:
                    address.click()
                    break
            # Click on the modal close button
            searchButton = driver.find_element(
                By.CSS_SELECTOR, 'div[data-testid="search-submit-button"]'
            ).click()
            time.sleep(2)
            try:
                driver.execute_script("window.scrollBy(0, window.innerHeight);")
                homestayFilter = WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located(
                        (
                            By.CSS_SELECTOR,
                            'div[data-testid="tvat-accomTypeFilter-HOMESTAY"]',
                        )
                    )
                )
                homestayFilter.click()
                time.sleep(2)
                WebDriverWait(driver, 10).until(
                    EC.visibility_of_element_located(
                        (By.CSS_SELECTOR, 'div[data-testid="tvat-searchListItem"]')
                    )
                )
                homelist = driver.find_elements(
                    By.CSS_SELECTOR, 'div[data-testid="tvat-searchListItem-content"]'
                )

                for homestay in homelist:
                    try:
                        home = HomestayItem()
                        home["homename"] = homestay.find_element(
                            By.CSS_SELECTOR, 'h3[data-testid="tvat-hotelName"]'
                        ).text.strip()
                        home["city"] = city
                        home["cheapest_price"] = (
                            homestay.find_element(
                                By.CSS_SELECTOR, 'div[data-testid="tvat-hotelPrice"]'
                            )
                            .text.strip()
                            .split(" ")[0]
                        )
                        homeid = homestay.find_element(
                            By.CSS_SELECTOR, 'div[data-testid="hotel-feature-section"]'
                        )
                        homeid = homeid.get_attribute("id")
                        homeid = homeid.split("-")[3]
                        base_url2 = "https://www.traveloka.com/vi-vn/hotel/detail?"
                        search_params2 = {
                            "spec": start_date.strftime("%d-%m-%Y")
                            + "."
                            + end_date.strftime("%d-%m-%Y")
                            + ".1.1.HOTEL."
                            + homeid
                            + "."
                            + home["homename"]
                            + ".1"
                        }
                        url2 = f"{base_url2}{urlencode(search_params2)}"
                        yield scrapy.Request(
                            url2, self.parse_home, cb_kwargs={"home": home, "url": url2}
                        )

                        driver2.get(url2)

                        # driver.execute_script("window.scrollBy(0, window.innerHeight);")
                        room_button = WebDriverWait(driver2, 10).until(
                            EC.visibility_of_element_located(
                                (By.CSS_SELECTOR, 'div[data-testid="link-ROOMS"]')
                            )
                        )
                        room_button.click()
                        time.sleep(2)
                        roomsContainer = driver2.find_element(
                            By.CSS_SELECTOR, 'div[data-testid="section-room-search"]'
                        )
                        roomTypes = roomsContainer.find_elements(
                            By.CSS_SELECTOR,
                            "h3.css-4rbku5.css-901oao.css-bfa6kz.r-cwxd7f.r-t1w4ow.r-adyw6z.r-b88u0q.r-135wba7.r-fdjqy7",
                        )
                        roomInfos = roomsContainer.find_elements(
                            By.CSS_SELECTOR,
                            "div.css-1dbjc4n.r-14lw9ot.r-1dzdj1l.r-xy67v1.r-6gpygo.r-1l7z4oj.r-ymttw5",
                        )
                        for roomType, roomInfo in zip(roomTypes, roomInfos):
                            room = RoomItem()
                            room["homename"] = home["homename"]
                            room["roomtype"] = roomType.text
                            text = roomInfo.text
                            size_match = re.search(r"(\d+\.\d+)\s*m²", text)
                            facilities_match = re.search(
                                r"\d+\.\d+\s*m²(.+?)Xem chi tiết phòng", text, re.DOTALL
                            )

                            sale_price_match = re.findall(
                                r"(\d{1,3}(?:\.\d{3})*|\d+)\s+VND", text
                            )
                            occupancy_match = re.search(r"(\d+)\s+khách", text)

                            # Extracted information
                            room_size = size_match.group(1) if size_match else None
                            room_facilities = (
                                facilities_match.group(1).strip().split("\n")
                                if facilities_match
                                else None
                            )
                            sale_price = (
                                sale_price_match[-1] if sale_price_match else None
                            )

                            roomOccu = roomInfo.find_element(
                                By.CSS_SELECTOR,
                                "div.css-1dbjc4n.r-9cy4zw.r-h1746q.r-1mdj7ya.r-15ik8dz.r-youum4.r-1l7z4oj.r-1e081e0.r-fd4yh7.r-136ojw6",
                            )
                            occupancy = len(
                                roomOccu.find_elements(
                                    By.CSS_SELECTOR, 'svg[data-id="IcUserAccountFill"]'
                                )
                            )

                            # Print the results
                            room["roomsize"] = room_size
                            room["facilities"] = room_facilities
                            room.addAvailable(
                                {"occupancy": occupancy, "price": sale_price}
                            )
                            self.room_collection.insert_one(room)
                    except NoSuchElementException:
                        continue
            except TimeoutException:
                continue
        driver2.quit()
        driver.quit()

    def parse_home(self, response, home, url):
        json_data = response.css("script#__NEXT_DATA__::text").get()
        if json_data:
            # Remove the surrounding script tags and load the JSON
            json_data = json_data.strip()
            json_data = json_data[json_data.find("{") : json_data.rfind("}") + 1]
            try:
                # Convert JSON to Python dictionary
                props_data = json.loads(json_data)
                hotel_data = (
                    props_data.get("props", {}).get("pageProps", {}).get("hotel", {})
                )
                review_data = (
                    props_data.get("props", {})
                    .get("pageProps", {})
                    .get("reviewSummary", {})
                    .get("reviewList", [])
                )
                home["url"] = url
                home["address"] = hotel_data.get("address")
                description = hotel_data.get("attribute", {}).get("description", "")
                soup = BeautifulSoup(description, "html.parser")
                home["description"] = " ".join(soup.stripped_strings).strip()
                home["rating"] = hotel_data.get("userRating")
                home["images"] = [
                    asset["url"] for asset in hotel_data.get("assets", [])
                ]
                home["property_highlights"] = [
                    facility["name"]
                    for facility in hotel_data.get("hotelFacilitiesTagDisplay", [])
                ]
                self.home_collection.insert_one(home)
                for review in review_data:
                    timestamp = (
                        int(review["timestamp"]) / 1000
                    )  # Assuming the timestamp is in milliseconds
                    review_date = datetime.utcfromtimestamp(timestamp).strftime(
                        "%Y-%m-%d"
                    )
                    self.review_collection.insert_one(
                        {
                            "homename": home["homename"],
                            "user_name": review["reviewerName"],
                            "review_date": review_date,
                            "review_score": review["overallScore"],
                            "review_content": review["reviewText"],
                        }
                    )
            except json.JSONDecodeError as e:
                self.log(f"Error decoding JSON: {e}")
