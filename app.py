from db import DB_USERNAME, DB_PWD, URL_LINK
import pymongo
URL = URL_LINK.format(
    DB_USERNAME, DB_PWD)
print(URL)
client = pymongo.MongoClient(URL)
db = client["Bookstore"]

pipeline = [{"$group": {"_id": "$isbn", "number_of_entries": {"$sum":1}}}]
result = db.books_selling_data.aggregate(pipeline)
#for books in result:
#    print(books)

pipeline = [{"$group": {"_id": "$isbn", "total_copies_sold": {"$sum":"$copies_sold"}}}, 
            {"$match": {"total_copies_sold":{"$gt":15000}}},
            {"$lookup": {"from": "books", "localField": "_id", "foreignField": "isbn", "as": "details"}},
            {"$project": {"_id":0, "details":{"title": 1}}}]
result = db.books_selling_data.aggregate(pipeline)
#for books in result:
#    print(books)

pipeline = [{"$match": {"author": "John Doe"}},
            {"$sort": {"isbn": 1}},
            {"$project": {"_id": 0, "isbn": 0}}]
result = db.books.aggregate(pipeline)
#for books in result:
#    print(books)

pipeline = [{"$addFields": {"price_per_book": {"$round": [{"$divide": [{"$toInt": "$total_price"}, {"$toInt": "$copies_sold"}]}, 2]}}},
            {"$project": {"_id":0, 'copies_sold': 0, 'total_price': 0, 'supplier': 0}}]
result = db.books_selling_data.aggregate(pipeline)
#for books in result:
#    print(books)

pipeline = [{"$unwind": "$supplier"}, 
            {"$match": {"supplier": "titan"}},
            {"$addFields": {"price_per_book": {"$round": [{"$divide": [{"$toInt": "$total_price"}, {"$toInt": "$copies_sold"}]}, 2]}}},
            {"$project": {"_id":0, 'copies_sold': 0, 'total_price': 0, 'supplier': 0}}]
result = db.books_selling_data.aggregate(pipeline)
#for books in result:
#    print(books)
