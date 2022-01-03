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

aggregation_1 = db.books_selling_data.aggregate([stage_1])


# print(list(aggregation_1))

# 2. For books with more than 15000 total copies sold, return all the titles in a single array

stage_2_1 = {
    "$group":
        {
            "_id": "$isbn",
            "total_copies": { "$sum": "$copies_sold" }
            
        }
}

stage_2_2 = {
    "$match":
        {
            "total_copies_sold": { "$gt": 15000 }
        }

}

stage_2_3 = {
    "$lookup": {
        "from": "books",
        "localField": "_id",
        "foreignField": "isbn",
        "as": "book_info"
    }
}

stage_2_4 = {
    "$project": {
        "_id": 0,
        "total_copies_sold": 0,
        "title": 1
    }
}


aggregation_2 = db.books_selling_data.aggregate([stage_2_1, stage_2_2, stage_2_3, stage_2_4])

# print(list(aggregation_2))

# 3. Find the number of books by John Doe, sorted by isbn (but isbn is not returned)

stage_3_1 = {
    "$match": {
        "author": "John Doe"
    }
}


stage_3_2 = {
    "$sort": {
        "$isbn": 1
    }
}

stage_3_3 = {
    "$project": {
        "isbn": 0, 
        "title": 1, 
        "author": 1
    }
}



aggregation_3 = db.books.aggregate([stage_3_1, stage_3_2, stage_3_3])

# print(len(list(aggregation_3)))

# 4. Return the price per book for each order


stage_4_1 = {
    "$addFields": {
        "price_per_book": {
            "$round": [{"$divide": ["$total_price", "$copies_sold"]}, 2]
            }
    }
}

stage_4_2 = {
    "$project": {
        "_id": 0,
        "order_id": 1,
        "copies_sold": 0,
        "total_price": 0,
        "supplier": 0
    }
}


aggregation_4 = db.books_selling_data.aggregate([stage_4_1, stage_4_2])

# print(list(aggregation_4))

# 5. Return the average price per book for each order supplied from titan (we want to sum up all the total copies sold and total price of all copies sold where titan is a supplier, but average.like weighted average for the number of bookd)

stage_5_1 = {
    "$unwind": "$supplier"
}

stage_5_2 = {
    "$group": {
        "_id": "$supplier",
        "overall_sale": {"$sum": "$copies_sold"},
        "overall_price": {"$sum": "total_price"}
    }
}

stage_5_3 = {
    "$match": {
        "supplier": "titan"
    }
}



stage_5_4 = {
    "addFields": {
            "average_price_per_book": {"round": [{"divide":["overall_price", "overall_sale"]}, 2]}
        }
}

stage_5_5 = {
    "$project": {
        "$_id": 1,
        "$average_price_per_book": 1
    }
}


aggregation_5 = db.books_selling_data.aggregate([stage_5_1, stage_5_2, stage_5_3, stage_5_4, stage_5_5])

# print(list(aggregation_5))