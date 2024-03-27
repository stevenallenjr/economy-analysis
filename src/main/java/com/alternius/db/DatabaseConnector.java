package com.alternius.db;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class to connect to PostgreSQL database using JDBC driver. Did not
 * implement connection pooling due to the single-threaded nature of this
 * implementation.
 */
public class DatabaseConnector {

	private Connection connection;

	/**
	 * Allows for connection to PostgreSQL database using the JDBC driver.
	 * 
	 * @param host     hostname/address of database server
	 * @param port     port of database server
	 * @param user     username of user to access database with
	 * @param password password of user to access database with
	 * @param database name of database to access
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public DatabaseConnector(String host, String port, String user, String password, String database)
			throws ClassNotFoundException, SQLException {
		// Initialize PostgreSQL driver and bind to JDBC
		Class.forName("org.postgresql.Driver");

		// Open connection to database
		connection = initializeConnection(host, port, user, password, database);
		connection.setAutoCommit(false);
	}

	/**
	 * Opens connection to database.
	 * 
	 * @param host     hostname/address of database server
	 * @param port     port of database server
	 * @param user     username of user to access database with
	 * @param password password of user to access database with
	 * @param database name of database to access
	 * @return Connection
	 * @throws SQLException
	 */
	private Connection initializeConnection(String host, String port, String user, String password, String database)
			throws SQLException {
		return DriverManager.getConnection(buildConnectionURL(host, port, database), user, password);
	}

	/**
	 * Builds connection URL using JDBC format
	 * 
	 * @param host     hostname of database server
	 * @param port     port of database server
	 * @param database name of database to access
	 * @return JDBC-formatted URI
	 */
	private String buildConnectionURL(String host, String port, String database) {
		System.out.println(String.format("jdbc:postgresql://%s:%s/%s", host, port, database));
		return String.format("jdbc:postgresql://%s:%s/%s", host, port, database);
	}

	/**
	 * Executes an SQL query on the connected database. Queries using this method
	 * MUST return results, or an error will be encountered.
	 * 
	 * @param query SQL query string
	 * @return ResultSet containing results from query
	 * @throws SQLException
	 */
	public List<Map<String, Object>> executeQuery(String query) throws SQLException {
		List<Map<String, Object>> resultList = new ArrayList<>();
		try (PreparedStatement statement = connection.prepareStatement(query);
				ResultSet rs = statement.executeQuery()) {
			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();
			while (rs.next()) {
				Map<String, Object> row = new HashMap<>();
				for (int i = 1; i <= columnCount; i++) {
					row.put(metaData.getColumnName(i), rs.getObject(i));
				}
				resultList.add(row);
			}
		}
		return resultList;
	}
	
	/**
	 * Executes an SQL query on the connected database. Queries using this method
	 * MUST return results, or an error will be encountered.
	 * 
	 * Accepts parameters for prepared statement construction.
	 * 
	 * @param query SQL query string
	 * @return ResultSet containing results from query
	 * @throws SQLException
	 */
	public List<Map<String, Object>> executeQuery(String query, Object... params) throws SQLException {
	    List<Map<String, Object>> resultList = new ArrayList<>();
	    try (PreparedStatement pstmt = connection.prepareStatement(query)) {
	        for (int i = 0; i < params.length; i++) {
	            pstmt.setObject(i + 1, params[i]);
	        }
	        try (ResultSet rs = pstmt.executeQuery()) {
	            ResultSetMetaData metaData = rs.getMetaData();
	            int columnCount = metaData.getColumnCount();
	            while (rs.next()) {
	                Map<String, Object> row = new HashMap<>();
	                for (int i = 1; i <= columnCount; i++) {
	                    row.put(metaData.getColumnName(i), rs.getObject(i));
	                }
	                resultList.add(row);
	            }
	        }
	    }
	    return resultList;
	}

	/**
	 * Executes an SQL update on the connected database. Queries using this method
	 * will not return results. Use for INSERT, UPDATE, or DELETE.
	 * 
	 * @param sql SQL query string
	 * @return int number of rows affected.
	 * @throws SQLException
	 */
	public int executeUpdate(String sql) throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			return statement.executeUpdate();
		}
	}
	
	/**
	 * Executes an SQL update on the connected database. Queries using this method
	 * will not return results. Use for INSERT, UPDATE, or DELETE.
	 * 
	 * Accepts parameters for prepared statement construction.
	 * 
	 * @param sql SQL query string
	 * @return int number of rows affected.
	 * @throws SQLException
	 */
	public int executeUpdate(String sql, Object... params) throws SQLException {
	    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
	        for (int i = 0; i < params.length; i++) {
	            pstmt.setObject(i + 1, params[i]);
	        }
	        return pstmt.executeUpdate();
	    }
	}

	/**
	 * Closes connection to database.
	 * 
	 * @throws SQLException
	 */
	public void closeConnection() throws SQLException {
		connection.close();
	}
	
	public void rollbackChanges() throws SQLException {
		connection.rollback();
	}
	
	public void commitChanges() throws SQLException {
		connection.commit();
	}
}
