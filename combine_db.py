import re
import json
import pymongo
import nltk
from nltk import word_tokenize
from pymongo import MongoClient
from bson import ObjectId

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017")
agoda = client["agoda_homestays"]
a_homes_collection = agoda["homes"]
a_rooms_collection = agoda["rooms"]
a_reviews_collection = agoda["reviews"]
booking = client["booking_homestays"]
b_homes_collection = booking["homes"]
b_rooms_collection = booking["rooms"]
b_reviews_collection = booking["reviews"]
traveloka = client["traveloka_homestays"]
t_homes_collection = traveloka["homes"]
t_rooms_collection = traveloka["rooms"]
t_reviews_collection = traveloka["reviews"]

combined_db = client["combined_db"]
combined_homes_collection = combined_db["homes"]
combined_homes_collection.drop()

combined_rooms_collection = combined_db["rooms"]
combined_rooms_collection.drop()


def clean_homename(homename):
    cleaned_homename = re.sub(r"(Phone|sđt).*", "", homename)
    return cleaned_homename.strip()


def search_home(city, homename, collection):
    match = re.match(r"^(.*?)\((.*?)\)$", homename)
    if match:
        outside_part = match.group(1).strip()
        inside_part = match.group(2).strip()
        cleaned_homename = clean_homename(outside_part)
        regex_pattern = re.compile(re.escape(cleaned_homename), re.IGNORECASE)

        result = (
            collection.find({"city": city, "homename": {"$regex": regex_pattern}})
            .sort("homename", pymongo.ASCENDING)
            .limit(1)
        )
        if not result:
            cleaned_homename = clean_homename(inside_part)
            regex_pattern = re.compile(re.escape(cleaned_homename), re.IGNORECASE)

            result = (
                collection.find({"city": city, "homename": {"$regex": regex_pattern}})
                .sort("homename", pymongo.ASCENDING)
                .limit(1)
            )
    else:
        cleaned_homename = clean_homename(homename)
        regex_pattern = re.compile(re.escape(cleaned_homename), re.IGNORECASE)

        result = (
            collection.find({"city": city, "homename": {"$regex": regex_pattern}})
            .sort("homename", pymongo.ASCENDING)
            .limit(1)
        )
    return list(result)


def search_room(homename, collection):
    result = collection.find({"homename": homename})
    return list(result)


def search_reviews(homename, collection):
    result = collection.find({"homename": homename})
    return list(result)


def convert_object_ids_to_strings(data):
    if isinstance(data, list):
        for i, item in enumerate(data):
            if isinstance(item, dict):
                convert_object_ids_to_strings(item)
            elif isinstance(item, ObjectId):
                data[i] = str(item)
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict):
                convert_object_ids_to_strings(value)
            elif isinstance(value, list):
                convert_object_ids_to_strings(value)
            elif isinstance(value, ObjectId):
                data[key] = str(value)


def shorten_room_type(room_type):
    # Convert to lower case for uniformity
    room_type_lower = room_type.lower()

    # Initialize a list to hold the translated keywords
    shortened_keywords = []
    keyword_mapping = {
        "bed in": ["giường trong phòng"],
        "deluxe double room": ["phòng đôi loại sang", "phòng deluxe giường đôi"],
        "deluxe room": [" phòng loại sang ", "phòng deluxe"],
        "balcony": ["ban công"],
        "standard": ["tiêu chuẩn"],
        "classic": ["cổ điển"],
        "double room": ["phòng đôi"],
        "single room": ["phòng đơn"],
        "triple room": ["phòng 3 người", "phòng ba người"],
        "quadruple room": ["phòng 4 người", " phòng bốn người"],
        "dormitory room": ["phòng tập thể"],
        "family room": ["phòng gia đình"],
        "king room": ["giường lớn", "giường cỡ king"],
        "queen room": ["giường cỡ queen", "giường đôi lớn"],
        "extra bed": ["giường phụ"],
        "private bathroom": ["phòng tắm riêng"],
    }
    for keyword, translations in keyword_mapping.items():
        if keyword in room_type_lower:
            shortened_keywords.append(keyword)
        else:
            # Check if any translation is in the room_type
            for translation in translations:
                if translation in room_type_lower:
                    shortened_keywords.append(keyword)
                    break  # No need to check further translations for this keyword

    # Join the keywords to form the shortened room type
    return " ".join(shortened_keywords)


def match_room(rooms_sources):
    matched_rooms = []
    added_rooms = []

    for selected_source_idx, selected_source in enumerate(rooms_sources):
        for room in selected_source:
            room_type = room["roomtype"].lower()
            if room["roomtype"] not in added_rooms:
                added_rooms.append(room["roomtype"])
                matched_room = {
                    "roomtype": room["roomtype"],
                    "photos": room.get("photos", []),
                    "facilities": room.get("facilities", []),
                    "availables": {
                        "agoda": None,
                        "booking": None,
                        "traveloka": None,
                    },
                }
                if selected_source_idx == 0:
                    matched_room["photos"] = room["photos"]
                    matched_room["availables"]["agoda"] = room["availables"]
                elif selected_source_idx == 1:
                    
                    matched_room["availables"]["booking"] = room["availables"]
                elif selected_source_idx == 2:
                
                    matched_room["availables"]["traveloka"] = room["availables"]

                for other_source_idx, other_source in enumerate(rooms_sources):
                    if other_source_idx != selected_source_idx:
                        for other_source_room in other_source:
                            other_source_room_type = other_source_room[
                                "roomtype"
                            ].lower()
                            if other_source_room["roomtype"] not in added_rooms:
                                if (
                                    (other_source_room_type in room_type)
                                    or (room_type in other_source_room_type)
                                    or (
                                        shorten_room_type(other_source_room_type)
                                        == shorten_room_type(room_type)
                                    )
                                ):
                                    if other_source_idx == 0:
                                        matched_room["photos"] = other_source_room[
                                            "photos"
                                        ]
                                        matched_room["availables"]["agoda"] = (
                                            other_source_room["availables"]
                                        )
                                    elif other_source_idx == 1:
                                        matched_room["availables"]["booking"] = (
                                            other_source_room["availables"]
                                        )
                                    elif other_source_idx == 2:
                                        matched_room["availables"]["traveloka"] = (
                                            other_source_room["availables"]
                                        )
                                    if len(
                                        other_source_room.get("facilities", [])
                                    ) > len(matched_room["facilities"]):
                                        matched_room["facilities"] = (
                                            other_source_room.get("facilities", [])
                                        )
                                    added_rooms.append(other_source_room["roomtype"])
                                    break
                matched_rooms.append(matched_room)
    return matched_rooms


def push_combined_home(
    homename,
    city,
    a_home,
    a_reviews,
    b_home,
    b_reviews,
    t_home,
    t_reviews,
    matched_rooms,
):
    combined_home = {
        "homename": homename,
        "city": city,
        "description": a_home.get("description", "")
        or b_home.get("description", "")
        or t_home.get("description", ""),
        "address": a_home.get("address", "")
        or b_home.get("address", "")
        or t_home.get("address", ""),
        "ratings": {
            "agoda": a_home.get("rating", "") if a_home else "",
            "booking": b_home.get("rating", "") if b_home else "",
            "traveloka": t_home.get("rating", "") if t_home else "",
        },
        "cheapest_prices": {
            "agoda": a_home.get("cheapest_price", "") if a_home else "",
            "booking": b_home.get("cheapest_price", "") if b_home else "",
            "traveloka": t_home.get("cheapest_price", "") if t_home else "",
        },
        "property_highlights": (
            a_home.get("property_highlights", [])
            or b_home.get("property_highlights", [])
            or t_home.get("property_highlights", [])
        ),
        "images": (
            a_home.get("images", [])
            or b_home.get("images", [])
            or t_home.get("images", [])
        ),
        "urls": {
            "agoda": a_home.get("url", ""),
            "booking": b_home.get("url", ""),
            "traveloka": t_home.get("url", ""),
        },
        "rooms": (matched_rooms),
        "reviews": {
            "agoda": a_reviews,
            "booking": b_reviews,
            "traveloka": t_reviews,
        },
    }

    combined_homes_collection.insert_one(combined_home)


added_homes = []
a_distinct_homenames = a_homes_collection.distinct("homename")

for homename in a_distinct_homenames:
    a_home = a_homes_collection.find({"homename": homename})[0]
    id = a_home.get("_id")
    if id not in added_homes:
        homename = a_home.get("homename")
        city = a_home.get("city")
        a_rooms = search_room(homename, a_rooms_collection)
        a_reviews = search_reviews(homename, a_reviews_collection)
        convert_object_ids_to_strings(a_rooms)
        convert_object_ids_to_strings(a_reviews)

        b_home = search_home(city, homename, b_homes_collection)
        b_rooms = []
        b_reviews = []
        if b_home:
            b_home = b_home[0]
            id = b_home.get("_id")
            if id not in added_homes:
                added_homes.append(id)
                matched_home = b_home
                b_rooms = search_room(matched_home.get("homename"), b_rooms_collection)
                b_reviews = search_reviews(
                    matched_home.get("homename"), b_reviews_collection
                )
                convert_object_ids_to_strings(b_rooms)
                convert_object_ids_to_strings(b_reviews)
        else:
            b_home = {}

        t_home = search_home(city, homename, t_homes_collection)
        t_rooms = []
        t_reviews = []
        if t_home:
            t_home = t_home[0]
            id = t_home.get("_id")
            if id not in added_homes:
                added_homes.append(id)
                matched_home = t_home
                t_rooms = search_room(matched_home.get("homename"), t_rooms_collection)
                t_reviews = search_reviews(
                    matched_home.get("homename"), t_reviews_collection
                )
                convert_object_ids_to_strings(t_rooms)
                convert_object_ids_to_strings(t_reviews)
        else:
            t_home = {}
        matched_rooms = match_room([a_rooms, b_rooms, t_rooms])
        push_combined_home(
            homename,
            city,
            a_home,
            a_reviews,
            b_home,
            b_reviews,
            t_home,
            t_reviews,
            matched_rooms,
        )
b_distinct_homenames = b_homes_collection.distinct("homename")

for homename in b_distinct_homenames:
    b_home = b_homes_collection.find({"homename": homename})[0]
    id = b_home.get("_id")
    if id not in added_homes:
        homename = b_home.get("homename")
        city = b_home.get("city")
        b_rooms = search_room(homename, b_rooms_collection)
        b_reviews = search_reviews(homename, b_reviews_collection)
        convert_object_ids_to_strings(b_rooms)
        convert_object_ids_to_strings(b_reviews)

        a_home = search_home(city, homename, a_homes_collection)
        a_rooms = []
        a_reviews = []
        if a_home:
            a_home = a_home[0]
            id = a_home.get("_id")
            if id not in added_homes:
                added_homes.append(id)
                matched_home = a_home
                a_rooms = search_room(matched_home.get("homename"), a_rooms_collection)
                a_reviews = search_reviews(
                    matched_home.get("homename"), a_reviews_collection
                )
                convert_object_ids_to_strings(a_rooms)
                convert_object_ids_to_strings(a_reviews)
        else:
            a_home = {}

        t_home = search_home(city, homename, t_homes_collection)
        t_rooms = []
        t_reviews = []
        if t_home:
            t_home = t_home[0]
            id = t_home.get("_id")
            if id not in added_homes:
                added_homes.append(id)
                matched_home = t_home
                t_rooms = search_room(matched_home.get("homename"), t_rooms_collection)
                t_reviews = search_reviews(
                    matched_home.get("homename"), t_reviews_collection
                )
                convert_object_ids_to_strings(t_rooms)
                convert_object_ids_to_strings(t_reviews)
        else:
            t_home = {}
        matched_rooms = match_room([a_rooms, b_rooms, t_rooms])

        push_combined_home(
            homename,
            city,
            a_home,
            a_reviews,
            b_home,
            b_reviews,
            t_home,
            t_reviews,
            matched_rooms,
        )

t_distinct_homenames = t_homes_collection.distinct("homename")

for homename in t_distinct_homenames:
    t_home = t_homes_collection.find({"homename": homename})[0]
    id = t_home.get("_id")
    if id not in added_homes:
        homename = t_home.get("homename")
        city = t_home.get("city")
        t_rooms = search_room(homename, t_rooms_collection)
        t_reviews = search_reviews(homename, t_reviews_collection)
        convert_object_ids_to_strings(t_rooms)
        convert_object_ids_to_strings(t_reviews)

        a_home = search_home(city, homename, a_homes_collection)
        a_rooms = []
        a_reviews = []
        if a_home:
            a_home = a_home[0]
            id = a_home.get("_id")
            if id not in added_homes:
                added_homes.append(id)
                matched_home = a_home
                a_rooms = search_room(matched_home.get("homename"), a_rooms_collection)
                a_reviews = search_reviews(
                    matched_home.get("homename"), a_reviews_collection
                )
                convert_object_ids_to_strings(a_rooms)
                convert_object_ids_to_strings(a_reviews)
        else:
            a_home = {}

        b_home = search_home(city, homename, b_homes_collection)
        b_rooms = []
        b_reviews = []
        if b_home:
            b_home = b_home[0]
            id = b_home.get("_id")
            if id not in added_homes:
                added_homes.append(id)
                matched_home = b_home
                b_rooms = search_room(matched_home.get("homename"), b_rooms_collection)
                b_reviews = search_reviews(
                    matched_home.get("homename"), b_reviews_collection
                )
                convert_object_ids_to_strings(b_rooms)
                convert_object_ids_to_strings(b_reviews)
        else:
            b_home = {}
        matched_rooms = match_room([a_rooms, b_rooms, t_rooms])

        push_combined_home(
            homename,
            city,
            a_home,
            a_reviews,
            b_home,
            b_reviews,
            t_home,
            t_reviews,
            matched_rooms,
        )
