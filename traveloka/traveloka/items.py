# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from typing import List


class HomestayItem(scrapy.Item):
    _id = scrapy.Field()
    url = scrapy.Field()
    homename = scrapy.Field()
    city = scrapy.Field()
    cheapest_price = scrapy.Field()
    rating = scrapy.Field()
    address = scrapy.Field()
    description = scrapy.Field()
    property_highlights = scrapy.Field()
    images = scrapy.Field()
    reviews = scrapy.Field()

    def addReview(self):
        self["reviews"] = self["reviews"] + 1

class RoomItem(scrapy.Item):
    _id = scrapy.Field()
    homename = scrapy.Field()
    roomtype = scrapy.Field()
    bedtype = scrapy.Field()
    roomsize = scrapy.Field()
    facilities = scrapy.Field()
    availables = scrapy.Field()

    def addAvailable(self, avai):
        # Check if the 'availables' field is already set and if it's a list
        if "availables" not in self or not isinstance(self["availables"], list):
            self["availables"] = []

        # Append the review to the 'availables' list
        self["availables"].append(avai)
