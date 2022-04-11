# Spark_Scala_Transaction_Analysis
This program helps you to understand the different analysis and forecast that can actually be done on a transaction file using spark with scala


Please find the semantics of the file below.

Transaction_Number -- Unique number for every transaction made.
Transaction_Date -- Date on which the transaction happened.
Userno -- Usernumber who made the transaction
Amount -- Price for which the transaction is made.
Product -- Name of the product purchased.
City -- City where the transaction happened.
Payment -- Payment type.


Use Cases:-
===========

1. Find out the average amount spent using cash and credit to make the transactions.
2. Find out the month of the year 2013 where transactions happened with the maximum amount.
3. Find out the city where the transaction happened with the least amount.
4. List down the top 2 product categories which has the maximum sales.

Tips to know:-
==============
1. Build all the related RDD's and enclosed them inside a single scala code.
2. Make sure that the final output directory that we building is not existing before.
3. When the final output generated is a sequence say list or string then convert them into RDD using sc.parallelize method, so that you can load the output into the final directory.
