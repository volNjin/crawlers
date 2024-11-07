import scrapy
from urllib.parse import urlencode
from urllib.parse import urlparse
from datetime import datetime, timedelta
import locale
from crawlers.items import HomestayItem
from crawlers.items import RoomItem
from pymongo import MongoClient
import json


class BookingSpider(scrapy.Spider):
    name = "booking"

    client = MongoClient("mongodb://localhost:27017")
    database_name = "booking_homestays"
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
    # allowed_domains = ["www.booking.com"]

    def start_requests(self):
        current_date = datetime.now()
        start_date = current_date + timedelta(days=1)
        end_date = current_date + timedelta(days=2)
        base_url = "https://www.booking.com/searchresults.vi.html?"
        with open("cities.json", "r", encoding="utf-8") as file:
            data = json.load(file)

        # Truy cập danh sách các tỉnh từ dữ liệu JSON
        cities = data["cities"]
        for city in cities:
            search_params = {
                "ss": city["en"],
                "checkin": start_date.strftime("%Y-%m-%d"),
                "checkout": end_date.strftime("%Y-%m-%d"),
                "group_adults": "1",
                "no_rooms": "1",
                "group_children": "0",
                "nflt": "ht_id=222",
            }
            url = f"{base_url}{urlencode(search_params)}"
            yield scrapy.Request(
                url,
                callback=self.parse,
                cb_kwargs={"city": city},
                dont_filter=True,
            )

    def parse(self, response, city):
        notice = response.css("h1.fa4a3a8221.ae40efd959 ::text").get()
        notice = notice.split(": ")[1]
        if notice != "Không tìm thấy chỗ nghỉ":
            for homestay in response.css('div[data-testid="property-card-container"]'):
                home = HomestayItem()
                home["city"] = city
                home["homename"] = homestay.css(
                    'a[data-testid="title-link"] div[data-testid="title"]::text'
                ).get()
                home["cheapest_price"] = (
                    homestay.css('span[data-testid="price-and-discounted-price"]::text')
                    .get()
                    .split("\xa0")[1]
                )
                rating = homestay.css('div[data-testid="review-score"] div::text').get()
                if rating is not None:
                    home["rating"] = rating.replace(",", ".")
                link = homestay.css('a[data-testid="title-link"]::attr(href)').get()
                yield response.follow(
                    link, self.parse_home, cb_kwargs={"home": home, "url": link}
                )

    def parse_home(self, response, home, url):
        home["url"] = url
        home["address"] = response.css("span.hp_address_subtitle::text").get().strip()
        home["description"] = (
            response.css(
                'div[data-capla-component-boundary="b-property-web-property-page/PropertyDescriptionDesktop"] p::text'
            )
            .get()
            .strip()
        )
        property_highlights = []
        for prop_highlight in response.css(
            'ul[class="aca0ade214 aaf30230d9 c2931f4182 e7d9f93f4d ed5cdd3fb3 fb5b81c565"] li'
        ):
            property_highlights.append(prop_highlight.css("::text").get().strip())
        home["property_highlights"] = property_highlights
        images = []
        for image in response.css(
            "div.nha_large_photo_main_content a.bh-photo-grid-item img"
        ):
            images.append(image.css("::attr(src)").get())
        home["images"] = images
        # Get home_name in url to send request to reviews pages
        parsed_url = urlparse(url)
        path_segments = parsed_url.path.split("/")
        home_name = path_segments[3].split(".")[0]
        total_reviews_text = response.css(
            "span.f13857cc8c.a5cc9f664c.c4b07b6aa8::text"
        ).getall()
        home["reviews"] = 0
        if total_reviews_text:
            total_reviews_text = total_reviews_text[1]
            total_reviews_text = total_reviews_text.replace(".", "")
            # Extract the digits from the text using regular expressions
            total_reviews = int(total_reviews_text.strip().split()[0])
            pages = int(total_reviews / 25)
            for page in range(0, pages + 1):
                review_link = "https://www.booking.com/reviewlist.vi.html?" + urlencode(
                    {
                        "type": "total",
                        "lang": "vn",
                        "sort": "f_recent_desc",
                        "cc1": "vn",
                        "dist": 1,
                        "pagename": home_name,
                        "rows": 25,
                        "offset": page * 25,
                    }
                )
                yield response.follow(
                    review_link,
                    self.parse_reviews,
                    cb_kwargs={"home": home, "total_reviews": total_reviews},
                )
        else:
            self.home_collection.insert_one(home)

        room = RoomItem()
        roomtype = ""
        for room_element in response.css("tr.js-rt-block-row"):
            if room_element.css("td.hprt-table-cell"):
                room["homename"] = home["homename"]
                roomtype = room_element.css(
                    "div.hprt-roomtype-block a.hprt-roomtype-link span.hprt-roomtype-icon-link::text"
                ).get()
                if roomtype:
                    room["roomtype"] = roomtype.strip()
                facilities_blocks = room_element.css("div.hprt-facilities-facility")
                if facilities_blocks:
                    roomsize = facilities_blocks.css(
                        'div.hprt-facilities-facility[data-name-en="room size"] span::text'
                    ).get()
                    if roomsize is not None:
                        roomsize = int(roomsize.split(" ")[0])
                        facilities_blocks.pop(0)

                    room["roomsize"] = roomsize

                    if facilities_blocks.css('div[data-name-en="privacy"]'):
                        facilities_blocks.pop(0)
                    room["facilities"] = facilities_blocks.css("::text").getall()

            if roomtype != "":
                room_id = room_element.css("::attr(data-block-id)").get()
                if room_id != "bbasic_0":
                    occupancy = room_id.split("_")[2]
                    price = room_element.css("::attr(data-hotel-rounded-price)").get()

                else:
                    occupancy = room_element.css(
                        "div.wholesalers_table__occupancy__icons.jq_tooltip span.bui-u-sr-only::text"
                    ).get()
                    if occupancy:
                        occupancy = occupancy.split(": ")[1]
                    price = (
                        room_element.css("div.bui-price-display__value::text")
                        .get()
                        .split("\xa0")
                    )
                avai = {
                    "occupancy": occupancy,
                    "price": price,
                }
                room.addAvailable(avai)

                if "hprt-table-last-row" in room_element.attrib["class"]:
                    self.room_collection.insert_one(room)
                    room = RoomItem()
                    roomtype = ""

    def parse_reviews(self, response, home, total_reviews):
        review_blocks = response.css("ul.review_list li")
        for block in review_blocks:
            user_avatar_img = block.css("img.bui-avatar__image::attr(src)").get()
            if user_avatar_img:
                user_info_block = block.css("div.bui-avatar-block__text")
                user_name = user_info_block.css(
                    "span.bui-avatar-block__title::text"
                ).get()
                user_country = user_info_block.css(
                    "span.bui-avatar-block__subtitle::text"
                ).get()
                review_detail = block.css("div.c-review-block__row")
                review_date = review_detail.css("span.c-review-block__date::text").get()
                # Set the locale to English (United States) to ensure full month name parsing
                locale.setlocale(locale.LC_TIME, "en_US.UTF-8")
                # Extract the date portion by removing the "Reviewed: " prefix
                date_part = review_date.replace("Reviewed: ", "").strip()
                # Parse the date string into a datetime object
                date_object = datetime.strptime(date_part, "%d %B %Y")
                # Format the datetime object as "yyyy-mm-dd"
                review_date = date_object.strftime("%Y-%m-%d")
                review_title = review_detail.css("h3.c-review-block__title::text").get()
                if review_title is not None:
                    review_title = review_title.strip()
                review_score = review_detail.css(
                    "div.bui-review-score__badge::text"
                ).get()
                if review_score is not None:
                    review_score = review_score.strip().replace(",", ".")
                review_content = review_detail.css(
                    "div.c-review span.c-review__body::text"
                ).getall()
                combined_review = ". ".join(
                    [
                        review.strip()
                        for review in review_content
                        if review.lower() != "n/a"
                        or review.lower() != "ko"
                        or review.lower() != "không"
                        or review.lower() != "no"
                        or review.lower() != "nothing"
                    ]
                )
                home.addReview()
                self.review_collection.insert_one(
                    {
                        "homename": home["homename"],
                        "user_name": user_name,
                        "user_country": user_country,
                        "review_date": review_date,
                        "review_title": review_title,
                        "review_score": review_score,
                        "review_content": combined_review,
                    }
                )

        if home["reviews"] == total_reviews:
            self.home_collection.insert_one(home)
