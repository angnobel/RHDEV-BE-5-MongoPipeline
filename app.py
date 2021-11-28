import pymongo
import datetime
from db import db

# Number of entries of selling data grouped by isbn
P1 = db.books_selling_data.aggregate([
    { "$group" : { "_id" : "$isbn", "number of entries" : { "$sum" : 1 }}}
    ])
# print (list(P1))


# For books with more than 15000 total copies sold, return all the titles in a single array
P2 = db.books_selling_data.aggregate([
    {"$group": {"_id": "$isbn", "copies" :{"$sum" : "$copies_sold"}}},
    {"$match": {"copies":{"$gte":15000}}},
    {"$lookup": { "from" : "books", "localField": "_id", "foreignField": "isbn", "as":">15000"}},
    { "$project" : { ">15000.title" : 1, "_id" : 0 } }
    ])
# print(list(P2))


# Find the number of books by John Doe, sorted by isbn (but isbn is not returned)
P3 = db.books.aggregate([
    { "$group" : { "_id" : "$isbn", "author" : { "$push" : "$author" }}},
    {"$match": {"author":"John Doe"}},
    {"$count": "JDbooks"}
    ])
# print(list(P3))


# Return the price per book for each order
P4 = db.books_selling_data.aggregate([
    {"$addFields":{"price per book": {"$divide":["$total_price" , "$copies_sold"]}}},
    {"$project":{"_id":0 , "order_id":1 , "price per book" : 1}}
    ])
# print(list(P4))


# Return the average price per book for each order supplied from titan
P5 = db.books_selling_data.aggregate([
    {"$addFields":{"price per book": {"$divide":["$total_price" , "$copies_sold"]}}},
    {"$project":{"_id":0 , "order_id":1 , "price per book" : 1, "supplier":1}},
    {"$unwind" : "$supplier"},
    {"$match": {"supplier":"titan"}}, 
    {"$group":{"_id":"$supplier", "average price per book": {"$avg": "$price per book"}}}
    ])
print(list(P5))
