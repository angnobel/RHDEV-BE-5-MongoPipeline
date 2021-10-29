# Your DB Code Here
from db import DB_PASSWORD, DB_USER, URL_LINK
import pymongo
from bson.objectid import ObjectId
import datetime

URL = URL_LINK.format(DB_USER, DB_PASSWORD)
client = pymongo.MongoClient(URL)
db = client["RHDEVS-BE-Mongo2"]

# 1. group bookSell data by isbn, count number of entries
stage1 = {"$group" : 
    {
        "_id": "$isbn", 
        "num_orders" : {"$sum": 1},
    }
}
# [print(x) for x in list(db.BooksSell.aggregate([stage1]))]



# 2. return all titles with books that sell more than 15000 total copies
stage21 = {"$group": 
    {
        "_id": "$isbn",
        "total_copies": {"$sum": "$copies_sold"}
    }
}
stage22 = { "$match" :
    {
        "total_copies": { "$gt" : 15000 }
    }
}
stage23 = {"$lookup":
    {
        "from": "Books",
        "localField": "_id",
        "foreignField": "isbn",
        "as": "bookInfo"
    }
}
stage24 = {"$addFields":
    {
        "title": "$bookInfo.title"
    }
}
stage25 = {"$project": 
    {
        "_id": 0,
        "title" : 1
    }
}

# print(list(db.BooksSell.aggregate([stage21, stage22, stage23, stage24, stage25])))



# 3. find all books by john doe, sorted by isbn
stage31 = {"$match": 
    {
        "author" : "John Doe"
    }
}
stage32 = {"$sort":
    {
        "isbn": pymongo.ASCENDING
    }
}
# tmp = list(db.Books.aggregate([stage31, stage32]))
# [print(x) for x in tmp]
# print(len(tmp))


# 4. return price per book for each order
stage41 = {"$project" : 
    {
        "_id": 0,
        "order_id": 1,
        "copies_sold": 1,
        "total_price": 1    
    }
}
stage42 = {"$addFields":
    {
        "price_per_book" : {"$round": [
            {"$divide": ["$total_price", "$copies_sold"]},
            2]
        }
            
    }
}
stage43 = {"$project" : 
    {
        "copies_sold": 0,
        "total_price": 0    
    }
}
# [print(x) for x in list(db.BooksSell.aggregate([stage41, stage42, stage43]))]


# 5. Return average price per book for each order supplied from titan
stage51 = {"$unwind": "$supplier"}
stage52 = {"$match": {"supplier": "titan"}}
stage53 = {"$group" : 
    {
        "_id": "$supplier",
        "overall_sold" : {"$sum": "$copies_sold"},
        "overall_price": {"$sum": "$total_price"}
    }
}
stage54 = {"$addFields":
    {"avg_price_per_book": 
        {"$round": [{"$divide": ["$overall_price", "$overall_sold"]}, 2]}
    }
}
stage55 = {"$project": 
    {
        "avg_price_per_book": 1
    }
}
# print(list(db.BooksSell.aggregate([stage51, stage52, stage53, stage54, stage55])))