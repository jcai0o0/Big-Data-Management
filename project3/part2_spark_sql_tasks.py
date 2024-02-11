from pyspark.sql import SparkSession

if __name__ == '__main__':
    my_spark = SparkSession.builder.getOrCreate()
    customers = my_spark.read.csv('/Users/janet/PycharmProjects/ds503_project3/customers.csv',
                                  header=True,
                                  inferSchema=True)
    purchases = my_spark.read.csv('/Users/janet/PycharmProjects/ds503_project3/purchases.csv',
                                  header=True,
                                  inferSchema=True)
    customers.createOrReplaceTempView('customers')
    purchases.createOrReplaceTempView('purchases')

    # Task 2.1 Filter out the Purchases with a total purchase amount above $600
    # store the result as T1
    query_1 = "SELECT TransID, CustID, TransTotal, ItemNum, TransDesc From purchases WHERE TransTotal <= 600"
    t1 = my_spark.sql(query_1)
    t1.show()
    t1.createOrReplaceTempView('T1')
    # write t1 as csv to local file system
    t1.write.option('header', True).csv('T1_output_file')

    # Task 2.2
    # Group T1 by the Number of Items purchases.
    # For each group calculate the median, min and max of total amount spend
    # Report the result back to the client side
    query_2 = "SELECT ItemNum, median(TransTotal), min(TransTotal), max(TransTotal) FROM T1 GROUP BY ItemNum"
    my_spark.sql(query_2).show()  # print out result in the terminal

    # Task 2.3
    # Group T1 by customer ID only for young customers between 18 and 25 years of age
    # For each group, report the customer ID, their age, total number of Items and total amount spend
    # Store the result as T3
    query_3 = """
              SELECT CustID, Age, SUM(TransTotal) as sumTransTotal, SUM(ItemNum) as sumItemNum FROM (
                 SELECT CustID, TransTotal, ItemNum, Name, Age 
                 FROM T1
                 JOIN customers 
                     ON T1.CustID=customers.ID 
                 WHERE Age BETWEEN 18 AND 25) as a
              GROUP BY CustID, Age
              """
    t3 = my_spark.sql(query_3)
    t3.show()
    t3.createOrReplaceTempView('T3')
    # write t1 as csv to local file system
    t3.write.option('header', True).csv('T3_output_file')

    # Task 2.4
    # Return all customer pairs IDs (C1 and C2) from T3 such that
    # C1 is younger than C2; C1 spend more than C2 but bought less items
    # Store the result as T4, and report it back to the client side
    query_4 = """
              SELECT a.CustID as C1_ID, b.CustID as C2_ID,
                     a.Age as Age1, b.Age as Age2,
                     a.sumTransTotal as TotalAmount1, b.sumTransTotal as TotalAmount2,
                     a.sumItemNum as TotalItemCount1, b.sumItemNum as TotalItemCount2
              FROM t3 a
              JOIN t3 b
                  ON a.Age < b.Age
              WHERE (a.sumTransTotal > b.sumTransTotal
                  AND a.sumItemNum < b.sumItemNum)
              """
    t4 = my_spark.sql(query_4)
    t4.show()  # report it back to the client side in terminal
    t4.createOrReplaceTempView('T4')
    # write t1 as csv to local file system
    t4.write.option('header', True).csv('T4_output_file')

    print("Part 2 Complete!")
