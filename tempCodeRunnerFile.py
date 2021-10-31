
            {"$addFields": {"price_per_book": {"$divide": [int("$total_price"), int("$copies_sold"