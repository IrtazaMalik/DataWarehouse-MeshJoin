package irtazaproject;

import com.opencsv.CSVReader;
import java.io.File;
import java.io.FileReader;
import java.sql.*;
import java.util.*;

public class IrtazaProjectDw {
    public static void main(String[] args) {
        
        String url = "jdbc:mysql://localhost:3306/metro_dw"; 
        String username = "root";  
        String password = "Brocode@69";  

        try (Connection conn = DriverManager.getConnection(url, username, password)) {
            System.out.println("Connected to the database.");

            
            loadTransactionsData(conn);
            loadProductsData(conn);
            loadCustomersData(conn);

      
            ensureDimCustomerData(conn);
            ensureDimProductData(conn);
            ensureDimStoreData(conn);
            ensureDimTimeData(conn);

            runMESHJOIN(conn);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void loadTransactionsData(Connection conn) {
        String csvFile = "C:\\Users\\Irtaza Malik\\Downloads\\transactions.csv"; 
        File file = new File(csvFile);
        if (!file.exists()) {
            System.out.println("File not found: " + csvFile);
            return;
        }

        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            String[] record;
            boolean isHeader = true;

            String query = "INSERT IGNORE INTO transactions (order_id, order_date, product_id, quantity, customer_id, time_id) VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                while ((record = reader.readNext()) != null) {
                    if (isHeader) {
                        isHeader = false;
                        continue;
                    }

                    int order_id = Integer.parseInt(record[0]);
                    Timestamp order_date = Timestamp.valueOf(record[1]);
                    int product_id = Integer.parseInt(record[2]);
                    int quantity = Integer.parseInt(record[3]);
                    int customer_id = Integer.parseInt(record[4]);
                    int time_id = Integer.parseInt(record[5]);

                    stmt.setInt(1, order_id);
                    stmt.setTimestamp(2, order_date);
                    stmt.setInt(3, product_id);
                    stmt.setInt(4, quantity);
                    stmt.setInt(5, customer_id);
                    stmt.setInt(6, time_id);
                    stmt.executeUpdate();
                }
                System.out.println("Data loaded successfully into the transactions table.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void loadProductsData(Connection conn) {
        String csvFile = "C:\\Users\\Irtaza Malik\\Downloads\\products_data.csv"; 
        File file = new File(csvFile);
        if (!file.exists()) {
            System.out.println("File not found: " + csvFile);
            return;
        }

        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            List<String[]> records = reader.readAll();
            records.remove(0);

            String query = "INSERT IGNORE INTO products (product_id, product_name, product_price, supplier_id, supplier_name, store_id, store_name) VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                for (String[] record : records) {
                    int product_id = Integer.parseInt(record[0]);
                    String product_name = record[1];
                    double product_price = Double.parseDouble(record[2].replace("$", "").replace(",", ""));
                    int supplier_id = Integer.parseInt(record[3]);
                    String supplier_name = record[4];
                    int store_id = Integer.parseInt(record[5]);
                    String store_name = record[6];

                    stmt.setInt(1, product_id);
                    stmt.setString(2, product_name);
                    stmt.setDouble(3, product_price);
                    stmt.setInt(4, supplier_id);
                    stmt.setString(5, supplier_name);
                    stmt.setInt(6, store_id);
                    stmt.setString(7, store_name);

                    stmt.executeUpdate();
                }
                System.out.println("Data loaded successfully into the products table.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void loadCustomersData(Connection conn) {
        String csvFile = "C:\\Users\\Irtaza Malik\\Downloads\\customers_data.csv"; 
        File file = new File(csvFile);
        if (!file.exists()) {
            System.out.println("File not found: " + csvFile);
            return;
        }

        try (CSVReader reader = new CSVReader(new FileReader(csvFile))) {
            List<String[]> records = reader.readAll();
            records.remove(0);

            String query = "INSERT IGNORE INTO customers (customer_id, customer_name, gender) VALUES (?, ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                for (String[] record : records) {
                    int customer_id = Integer.parseInt(record[0]);
                    String customer_name = record[1];
                    String gender = record[2];

                    stmt.setInt(1, customer_id);
                    stmt.setString(2, customer_name);
                    stmt.setString(3, gender);

                    stmt.executeUpdate();
                }
                System.out.println("Data loaded successfully into the customers table.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void ensureDimCustomerData(Connection conn) throws SQLException {
        String query = "INSERT IGNORE INTO dim_customers (customer_id, customer_name, gender) " +
                       "SELECT DISTINCT t.customer_id, c.customer_name, c.gender " +
                       "FROM transactions t " +
                       "JOIN customers c ON t.customer_id = c.customer_id " +
                       "WHERE c.customer_name IS NOT NULL AND c.gender IS NOT NULL " +
                       "AND t.customer_id NOT IN (SELECT customer_id FROM dim_customers)";
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.executeUpdate();
            System.out.println("Ensured all customers are in dim_customers.");
        }
    }

    private static void ensureDimProductData(Connection conn) throws SQLException {
        String query = "INSERT IGNORE INTO dim_products (product_id, product_name, product_price, supplier_id, supplier_name, store_id) " +
                       "SELECT DISTINCT p.product_id, p.product_name, p.product_price, p.supplier_id, p.supplier_name, p.store_id " +
                       "FROM products p " + 
                       "WHERE p.product_id NOT IN (SELECT product_id FROM dim_products)";
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.executeUpdate();
            System.out.println("Ensured all products are in dim_products.");
        }
    }

    private static void ensureDimStoreData(Connection conn) throws SQLException {
        String query = "INSERT IGNORE INTO dim_stores (store_id, store_name) " +
                       "SELECT DISTINCT p.store_id, p.store_name " +
                       "FROM products p " + 
                       "WHERE p.store_id NOT IN (SELECT store_id FROM dim_stores)";
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.executeUpdate();
            System.out.println("Ensured all stores are in dim_stores.");
        }
    }

    private static void ensureDimTimeData(Connection conn) throws SQLException {
        String query = "INSERT IGNORE INTO dim_time (time_id, day, month, quarter, year) " +
                       "SELECT DISTINCT " +
                       "  EXTRACT(DAY FROM t.order_date) AS day, " +
                       "  EXTRACT(MONTH FROM t.order_date) AS month, " +
                       "  QUARTER(t.order_date) AS quarter, " +
                       "  EXTRACT(YEAR FROM t.order_date) AS year, " +
                       "  EXTRACT(YEAR FROM t.order_date) * 10000 + EXTRACT(MONTH FROM t.order_date) * 100 + EXTRACT(DAY FROM t.order_date) AS time_id " +
                       "FROM transactions t " +
                       "WHERE t.order_date IS NOT NULL " +
                       "AND NOT EXISTS (SELECT 1 FROM dim_time dt WHERE dt.time_id = EXTRACT(YEAR FROM t.order_date) * 10000 + EXTRACT(MONTH FROM t.order_date) * 100 + EXTRACT(DAY FROM t.order_date))"; 
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.executeUpdate();
            System.out.println("Ensured all time data is in dim_time.");
        }
    }

    private static void runMESHJOIN(Connection conn) {
        try {
            Map<Integer, Transaction> transactions = loadTransactionsFromStaging(conn);
            List<Map<Integer, Product>> productPartitions = loadProductPartitions(conn, 100);

            String insertQuery = "INSERT IGNORE INTO sales_facts (order_id, customer_id, product_id, store_id, order_date, total_sale) VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(insertQuery)) {
                for (Map<Integer, Product> partition : productPartitions) {
                    for (Transaction transaction : transactions.values()) {
                        Product product = partition.get(transaction.productId);
                        if (product != null) {
                            double totalSale = transaction.quantity * product.productPrice;

                            stmt.setInt(1, transaction.orderId);
                            stmt.setInt(2, transaction.customerId);
                            stmt.setInt(3, transaction.productId);
                            stmt.setInt(4, product.storeId);
                            stmt.setTimestamp(5, transaction.orderDate);
                            stmt.setDouble(6, totalSale);

                            stmt.addBatch();
                        }
                    }
                    stmt.executeBatch();
                }
            }
            System.out.println("Data loaded into sales_facts.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static Map<Integer, Transaction> loadTransactionsFromStaging(Connection conn) throws SQLException {
        String query = "SELECT * FROM transactions";
        Map<Integer, Transaction> transactions = new HashMap<>();

        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                transactions.put(rs.getInt("order_id"),
                    new Transaction(rs.getInt("order_id"), rs.getInt("product_id"),
                        rs.getInt("customer_id"), rs.getInt("quantity"), rs.getTimestamp("order_date")));
            }
        }
        return transactions;
    }

    private static List<Map<Integer, Product>> loadProductPartitions(Connection conn, int partitionSize) throws SQLException {
        String query = "SELECT * FROM products";
        List<Map<Integer, Product>> partitions = new ArrayList<>();

        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            Map<Integer, Product> partition = new HashMap<>();
            int count = 0;

            while (rs.next()) {
                partition.put(rs.getInt("product_id"), 
                    new Product(rs.getInt("product_id"), rs.getString("product_name"),
                        rs.getDouble("product_price"), rs.getInt("supplier_id"),
                        rs.getString("supplier_name"), rs.getInt("store_id")));
                if (++count == partitionSize) {
                    partitions.add(partition);
                    partition = new HashMap<>();
                    count = 0;
                }
            }
            if (!partition.isEmpty()) partitions.add(partition);
        }
        return partitions;
    }
}

class Transaction {
    int orderId, productId, customerId, quantity;
    Timestamp orderDate;

    public Transaction(int orderId, int productId, int customerId, int quantity, Timestamp orderDate) {
        this.orderId = orderId;
        this.productId = productId;
        this.customerId = customerId;
        this.quantity = quantity;
        this.orderDate = orderDate;
    }
}

class Product {
    int productId, supplierId, storeId;
    String productName, supplierName;
    double productPrice;

    public Product(int productId, String productName, double productPrice, int supplierId, String supplierName, int storeId) {
        this.productId = productId;
        this.productName = productName;
        this.productPrice = productPrice;
        this.supplierId = supplierId;
        this.supplierName = supplierName;
        this.storeId = storeId;
    }
}