# Your DB Code Here

from db import db
from db import DB_PASSWORD, DB_USERNAME, URL_LINK
import pymongo
from bson.objectid import ObjectId
import datetime

URL = URL_LINK.format(DB_USERNAME, DB_PASSWORD)
client = pymongo.MongoClient(URL)
db = client["RHDEVS-BE-Mongo"]

# 1. Number of entries of selling data grouped by isbn

stage_1 = {
    "$group":
        {
            "_id": "$isbn",
            "order_count": {"$sum": 1},
        }
}

# 2. For books with more than 15000 total copies sold, return all the titles in a single array
# 3. Find the number of books by John Doe, sorted by isbn (but isbn is not returned)
# 4. Return the price per book for each order
# 5. Return the average price per book for each order supplied from titan (we want to sum up all the total copies sold and total price of all copies sold where titan is a supplier, but average.like weighted average for the number of bookd)