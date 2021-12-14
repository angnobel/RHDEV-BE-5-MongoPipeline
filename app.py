import pymongo
import datetime
from db import * 

db = rhdevsdb

# 1.Number of entries of selling data grouped by isbn
y = db.books_selling_data.aggregate(
    [{"$group": { "_id": "$isbn", "count" : {"$sum" : 1}}}]
    )

print(list(y))


#2. For books with more than 15000 total copies sold, return all the titles in a single array
y = db.books_selling_data.aggregate(
    [{"$group": { "_id": "$isbn", "total_copies" : {"$sum" : "$copies_sold"}}},
    {"$match": { "total_copies" : {"$gt" : 15000 }}},
    {"$lookup": { "from" : "books", "localField": "_id", "foreignField": "isbn", "as":"filtered"}},
    { "$project" : { "filtered.title" : 1, "_id" : 0 } }
    ])

print(list(y))

# 3. Find the number of books by John Doe, sorted by isbn (but isbn is not returned)

y = db.books.aggregate(
    [{"$group": { "_id": "$isbn", "author" : { "$push" : "$author" }, "title" : { "$push" : "$title" }}},
    {"$match" : { "author" : "John Doe"}},
    { "$sort": { "_id": 1}},
    { "$project" : { "title" : 1, "_id" : 0 }},
    ])

print(list(y))


#4. Return the price per book for each order

y = db.books_selling_data.aggregate(
    [{"$addFields" : {"price_per_book ": {"$divide": ["$total_price", "$copies_sold"]}}},
    {"$project" : { "order_id" : 1, "price_per_book" : 1 }}
    ])

print(list(y))


#5. Return the average price per book for each order supplied from titan

y = db.books_selling_data.aggregate(
    [{"$addFields" : {"price_per_book ": {"$divide": ["$total_price", "$copies_sold"]}}},
    { "$unwind" : "$supplier"},
    { "$match" : { "supplier" : "titan"}},
    {"$group":{"_id":"$supplier", "average price per book": {"$avg": "$price per book"}}}
    ])

print(list(y))

