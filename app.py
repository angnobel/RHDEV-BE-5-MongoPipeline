# Your DB Code Here
# Retrieve the following data using 1 aggregate call

# Number of entries of selling data grouped by isbn
pipeline1 = [
    {"$group" : {"_id" : "$isbn", "total_entries" : {"$sum" : 1} }}
]
db.bookSellingData.aggregate(pipeline1)
# For books with more than 15000 total copies sold, return all the titles in a single array
pipeline2 = [
    {"$group" : {"_id" : "$isbn", "total_copies_sold" : {"$sum" : "$copies_sold"} }}, 
    {"$match" : { "total_copies_sold" : {"$gt" : 15000} }},
    {
        "$lookup" :
        {
            from "books",
            localField : "_id" ,
            foriegnField : "title" ,
            as : "bookInfo"
        }
    },
    {"$project" : {"_id" : 0, "bookInfo" : {"title" : 1}}} 
]
db.bookSellingData.aggregate(pipeline2)

#Find the number of books by John Doe, sorted by isbn (but isbn is not returned)
pipeline3 = [
    {"$match" : {"author" : "John Doe"}}, 
    {"$sort" : {"isbn" : 1}},
    {"$project" : {"_id" : 0, "isbn" : 0}} # the remaining fields are allocated 1
]
db.books.aggregate(pipeline3)

#Return the price per book for each order
pipeline4 = [
    {
        "$addFields" : 
            {"price_per_book" : {"$divide" : ["$total_price", "$copies_sold"] }}
    },
    {"$project" : {"_id" : 0, "price_per_book" : 1, "isbn" : 1}} 
]
db.bookSellingData.aggregate(pipeline4)

#Return the average price per book for each order supplied from titan
pipeline5 = [
    {"$unwind" : "$supplier"},
    {"$match" : {"supplier" : "titan"}},
    {"$group" : {"_id" : "$isbn", "total_copies_sold" : {"$sum" : "$copies_sold"}, "total_money_earned" : {"$sum" : "$total_price"} }},
    {
        "$addFields" :
            {"average_price" {"$divide" : ["$total_money_earned", "total_copies_sold"] }}
    },
    {"$project" : {"_id" : 0, "average_price" : 1 }}
]
db.bookSellingData.aggregate(pipeline5)