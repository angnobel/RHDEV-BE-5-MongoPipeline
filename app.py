from db import db
import json
from pathlib import Path

#################################################################
#               jsonDecoder (Created for SOC and DRY)           #
#################################################################
def jsonDecoder(file_object):
    try:
        f = open(file_object)
        data = json.load(f)
        return data
    except Exception:
        return "Error in Decoding JSON"

##########################
#       books.json       #
##########################
def create(file_object):
    try:
        data = jsonDecoder(file_object)
        db.books.insertMany(data)
        return "Successfully imported the file to MongoDB"
    except Exception:
        return "Cannot import JSON File"

#################################
#   books_selling_data.json     #
#################################
def sales(file_object):
    try:
        data = jsonDecoder(file_object)
        db.booksSale.insertMany(data)
        return "Successfully imported the file to MongoDB"
    except Exception:
        return "Cannot import JSON File"

############################
#        Question 1        #
############################
def grouping():
    try:
        pipeline = [{
            "$group": {"_id": "$isbn", "count": {"$count": {}}}
        }]
        return list(db.booksSale.aggregate(pipeline))
    except Exception:
        return "Error in performing action"

# Expected Output:
# {
#    "_id": "978-3-16-148410-0", "count": 2,
#    "_id": "978-3-16-148410-0", "count": 1
# }

############################
#        Question 2        #
############################
def sorting():
    try:
        return "" 
    except Exception:
        return "Error in performing action"