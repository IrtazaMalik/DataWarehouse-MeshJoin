CREATE DATABASE IF NOT EXISTS metro_dw;
USE metro_dw;

CREATE TABLE IF NOT EXISTS transactions (
    order_id INT PRIMARY KEY,
    order_date DATETIME NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    customer_id INT NOT NULL,
    time_id INT NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_price DECIMAL(10, 2) NOT NULL,
    supplier_id INT NOT NULL,
    supplier_name VARCHAR(255) NOT NULL,
    store_id INT NOT NULL,
    store_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    gender VARCHAR(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    gender VARCHAR(10) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_price DECIMAL(10, 2) NOT NULL,
    supplier_id INT NOT NULL,
    supplier_name VARCHAR(255) NOT NULL,
    store_id INT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_stores (
    store_id INT PRIMARY KEY,
    store_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id INT PRIMARY KEY,
    day INT NOT NULL,
    month INT NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL
);

CREATE TABLE IF NOT EXISTS sales_facts (
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    store_id INT NOT NULL,
    order_date DATETIME NOT NULL,
    total_sale DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (store_id) REFERENCES dim_stores(store_id)
);

SELECT * FROM sales_facts LIMIT 10;
SELECT * FROM dim_time LIMIT 10;
SELECT * FROM dim_customers LIMIT 10;
SELECT * FROM dim_products LIMIT 10;
SELECT * FROM dim_stores LIMIT 100;



