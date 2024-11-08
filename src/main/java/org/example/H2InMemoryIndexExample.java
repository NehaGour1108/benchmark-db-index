package org.example;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

public class H2InMemoryIndexExample {

    public static void main(String[] args) {
        // Connect to H2 in-memory database
        String url = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";  // In-memory database

        // Record the start time for index creation
        long startTime = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(url, "sa", "");
             Statement stmt = conn.createStatement()) {

            // Create two tables
            stmt.execute("CREATE TABLE IF NOT EXISTS Table1 (ID INT AUTO_INCREMENT PRIMARY KEY, Age INT)");
            stmt.execute("CREATE TABLE IF NOT EXISTS Table2 (ID VARCHAR(16) PRIMARY KEY, Age INT)");

            // Insert 1 million rows into each table
            for (int i = 0; i < 1_000_000; i++) {
                stmt.execute("INSERT INTO Table1 (Age) VALUES (" + (20 + (i % 50)) + ")");
                stmt.execute("INSERT INTO Table2 (ID, Age) VALUES ('ID_" + i + "', " + (20 + (i % 50)) + ")");
            }

            // Create index on the age column of both tables
            stmt.execute("CREATE INDEX idx_age_table1 ON Table1 (Age)");
            stmt.execute("CREATE INDEX idx_age_table2 ON Table2 (Age)");

            // Measure time taken to create indexes
            long endTime = System.currentTimeMillis();
            System.out.println("Time taken to create indexes: " + (endTime - startTime) + " ms");

            // Query to check the data with the index
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM Table1 WHERE Age = 30");
            if (rs.next()) {
                System.out.println("Count of rows with Age 30 in Table1: " + rs.getInt(1));
            }

            rs = stmt.executeQuery("SELECT COUNT(*) FROM Table2 WHERE Age = 30");
            if (rs.next()) {
                System.out.println("Count of rows with Age 30 in Table2: " + rs.getInt(1));
            }

            // Query to get index information from H2's INFORMATION_SCHEMA
            ResultSet indexRs = stmt.executeQuery("SELECT * FROM INFORMATION_SCHEMA.INDEXES WHERE TABLE_NAME = 'TABLE1'");
            while (indexRs.next()) {
                System.out.println("Index: " + indexRs.getString("INDEX_NAME") + " on Table1");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}