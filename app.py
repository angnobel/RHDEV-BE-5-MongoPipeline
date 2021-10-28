URL = DB_LINK.format(
    DB_USERNAME, DB_PWD)

client = pymongo.MongoClient(URL)
db = client["RHDEVS-BE-Mongo"]


#1
pipeline1 = [{"$group": 
            {"_id": "$isbn", 
            "number_of_entries" : {"$sum":1}}}]
print(list(db.sellingdata.aggregate(pipeline1)))

#2
pipeline2 = [{"$group" : {"_id": "$isbn","number_of_copies": {"$sum": "$copies_sold"}}},
              {"$project" : {"_id" : 1,"number_of_copies" : 1}},
              {"$match" : {"number_of_copies" : {"$gte" : 15000}}},
              {"$lookup" : {"from": 'books', "localField": '_id', "foreignField": 'isbn',"as": 'details'}},
              {"$project" : {"number_of_copies" : 0, "_id" :0}},
              {"$set" : {"titledetails" : "$details.title"}},
              {"$project" : {"details":0}}
              ]

print(list(db.sellingdata.aggregate(pipeline2)))

#3
print(list(db.books.find({"author":"John Doe"})))

pipeline3 = [{"$group": {"_id": "$author","isbn" : {"$addToSet" : "$isbn"},"titles": {"$addToSet": "$title"},  "number_of_books": {"$sum":1}}},
              {"$sort": {"isbn":1}},
              {"$project" : {"isbn" : 0}},
              {"$match" : {"_id":"John Doe"}},
              ]

print(list(db.books.aggregate(pipeline3)))

#4
pipeline4 = [{"$addFields":{"price_per_book": {"$divide": ["$total_price","$copies_sold"]}}},
              {"$project":{"_id":0, "order_id":1, "price_per_book":{"$round" : ["$price_per_book",2]}}}]

print(list(db.sellingdata.aggregate(pipeline4)))

#5

pipeline5 = [{"$unwind" : {"path": "$supplier"}},
              {"$match" : {"supplier" : "titan"}},
              {"$group" : {"_id": "$supplier", "total_copies" : {"$sum" : "$copies_sold"},  "sum_price" : {"$sum": "$total_price"}}},
              {"$addFields": {"average_per_book" : {"$divide" : ["$sum_price","$total_copies"]}}},
              {"$project": {"_id":1, "average_per_book" : {"$round" : ["$average_per_book",2]}}}]

print(list(db.sellingdata.aggregate(pipeline5)))
