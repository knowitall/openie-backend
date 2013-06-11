package edu.knowitall.browser.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DerbyDb {
    public static final String PROTOCOL = "jdbc:derby://";
    public static final String DRIVER = "org.apache.derby.jdbc.ClientDriver";
    protected Connection connection = null;

    /**
     * Creates a new Derby DB connection.
     * 
     * @param url The URL of the database (e.g., localhost:1527://path/to/db)
     */
    public DerbyDb(String url) {
        String connectionUrl = PROTOCOL + url;
        try {
            Class.forName(DRIVER);
            connection = DriverManager.getConnection(connectionUrl);
        } catch (SQLException e) {
            throw new RuntimeException("Could not open Derby DB at " + connectionUrl, e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Could not find Derby driver " + DRIVER, e);
        }
    }

    /**
     * Closes the Derby database connection.
     */
    public void cleanUp() {
      try {
        connection.close();
      } catch (SQLException e) {
        throw new RuntimeException("Error closing Crosswikis DB connection.", e);
      }
    }
}
