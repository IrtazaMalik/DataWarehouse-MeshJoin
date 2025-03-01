# DataWarehouse-MeshJoin

## Overview:
This project is focused on building a Data Warehouse (DW) for a retail business, which involves
the use of Star Schema to structure the data efficiently for reporting and analysis. The key
objective is to analyze sales data using OLAP queries and to implement an ETL process for
transforming and loading data into the warehouse. We also explore the MESHJOIN algorithm
for optimizing the ETL process by processing large datasets in partitions, specifically using
disk-buffer, stream-buffer, hash-table, and queue concepts.
The project aims to:
● Load sales data into a structured star schema data warehouse.
● Analyze sales, product, customer, and store data through SQL queries.
● Implement MESHJOIN to optimize data processing during ETL.

## 2. Data Warehouse Schema (Star Schema):
Fact Table:
● sales_facts: Contains transaction data (sales data) with foreign keys linking to the
dimension tables.
Dimension Tables:
● dim_customers: Contains customer details (ID, name, gender).
● dim_products: Contains product details (ID, name, price, supplier).
● dim_stores: Contains store details (ID, name).
● dim_time: Contains time information (day, month, quarter, year).
## 3. MeshJoin Algorithm:
The key steps in MESHJOIN:
● Load a new chunk of customer transactions into the stream-buffer.
● Replace the product partition in the disk-buffer.
● Join customer transactions with the product partition.
● After each cycle, remove the oldest chunk of customer transactions from the
hash table and queue.
How it works:
● Disk-Buffer: Loads product data in chunks (partitions) into memory, represented
by productPartitions.
● Stream-Buffer: Holds chunks of customer transaction data in memory,
represented by the transactionQueue.
● Hash Table: Stores customer transactions for quick lookup and joining with
product data.
● Queue: Ensures each chunk of customer transactions is processed and joined
with the entire product data before being removed in a FIFO order.
## 4. Shortcomings of Meshjoin:
● Memory Consumption: Despite the use of buffers, memory consumption can still be
high for very large datasets. If the buffer size is not properly optimized, the system may
experience performance degradation.
● Complexity in Managing Buffers: The cyclic nature of the buffer management can be
complex, especially when the data partitions need to be dynamically adjusted.
Mismanagement of buffer sizes can lead to data inconsistencies or slower processing.
● Not Suitable for Small Datasets: MESHJOIN is designed for large-scale data
processing. For smaller datasets, traditional join methods (like hash joins) are more
efficient. Therefore, using MESHJOIN on small data might introduce unnecessary
overhead.
## 5. Learning from project:
● The importance of star schema in organizing data efficiently for business intelligence
and reporting.
● How to implement OLAP queries for complex data analysis, enabling meaningful
insights from large transactional datasets.
● The MESHJOIN algorithm and its practical implementation to optimize data processing
in ETL systems, especially for large-scale data in a multidimensional context.
● The key differences between traditional joins and MESHJOIN in terms of memory
management and computational efficiency.
Additionally, this project improved my skills in:
● SQL for querying and data manipulation.
● Java programming for database connections and ETL implementation.
● Working with large datasets and optimizing performance through algorithms like
MESHJOIN.

## 8. Conclusion:
This project helped me understand how data warehouses are designed, implemented,
and used for business analytics. The Star Schema architecture enables fast querying of
large datasets, and the MESHJOIN algorithm provides a way to efficiently handle huge
amounts of transactional data during the ETL process. By implementing these concepts,
I’ve gained a deeper understanding of data warehousing and optimization techniques..


# Readme :

Steps for the project:


1 - run starschema.sql to create the data warehouse.

2 - run IrtazaProjectDw.java for the whole ETL process which will insert the data into the datawarehouse.

3 - run project olap queries.sql to retrieve insights from the data warehouse.
