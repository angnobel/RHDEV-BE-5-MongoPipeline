import pymongo

dbUsername = ""
dbPassword = ""

###############################################################
#           db.py key: Lesson Learnt -- Do not share          #
###############################################################
URL = "mongodb+srv://" + dbUsername + ":" + dbPassword + "@mycluster.vv19r.mongodb.net/FoodOrderDemo?retryWrites=true&w=majority"
client = pymongo.MongoClient(URL)
db = client["Books"]

#####################################
#        Question 1 Solution        #
#####################################
def questionOne():
    try:
        pipeline = [{
            "$group": 
            {
                "_id": "$isbn", 
                "count": {"$sum": "$copies_sold"}
                }
        }]
        return list(db.booksSales.aggregate(pipeline))
    except Exception:
        return "Error in performing action"

#####################################
#        Question 2 Solution        #
#####################################
def questionTwo():
    try:
        stageOne = {"$group": {"_id": "$isbn", "total_copies_sold": {"$sum": "$copies_sold"}}}
        stageTwo = {"$match": {"total_copies_sold": {"$gt": 15000}}}
        # Do note that look up works only when the localField matches the foreignField
        stageThree = {"$lookup": {"from": "books", "localField": "_id", "foreignField": "isbn", "as": "Titles"}}
        stageFour = {"$project": {"_id": 0, "total_copies_sold": 0}}
        # stageFive = {"$unwind": "$Titles"} <-- Unwinding does not work as it will lead to a dict in a dict
        pipeline = [stageOne, stageTwo, stageThree, stageFour]
        return list(db.booksSales.aggregate(pipeline))
    except Exception:
        return "Error in performing action"

#####################################
#        Question 3 Solution        #
#####################################

def questionThree():
    try:
        stageOne = {"$match": {"author": "John Doe"}}
        stageTwo = {"$project": {"_id": 0, "title": 0, "author": 0}}
        stageThree = {"$sort": {"isbn": 1}}
        pipeline = [stageOne, stageTwo, stageThree]
        return list(db.books.aggregate(pipeline))
    except Exception:
        return "Error in performing action"

#####################################
#        Question 4 Solution        #
#####################################
def questionFour():
    try:
        stageOne = {"$addFields": {"price_per_book": {"$divide": ["$total_price", "$copies_sold"]}}}
        stageTwo = {"$project": {"_id": 0, "isbn": 0, "supplier": 0, "total_price": 0, "copies_sold": 0}}
        pipeline = [stageOne, stageTwo]
        return list(db.booksSales.aggregate(pipeline))
    except Exception:
        return "Error in performing action"

#####################################
#        Question 5 Solution        #
#####################################
def questionFive():
    try:
        stageOne = {"$unwind": "$supplier"}
        stageTwo = {"$addFields": {"price_per_book": {"$divide": ["$total_price", "$copies_sold"]}}}
        stageThree = {"$match": {"supplier": "titan"}}
        stageFour = {"$project": {"_id": 0, "copies_sold": 0, "total_price": 0, "isbn": 0}}
        pipeline = [stageOne, stageTwo, stageThree, stageFour]
        return list(db.booksSales.aggregate(pipeline))
    except Exception:
        return "Error in performing action"
