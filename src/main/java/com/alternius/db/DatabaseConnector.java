package com.alternius.db;

import java.sql.*;

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
	public ResultSet executeQuery(String query) throws SQLException {
		if (connection == null)
			// Lazy error handling, verifies a connection exists before an SQLException gets
			// thrown
			return null;

		Statement statement = connection.createStatement();
		return statement.executeQuery(query);
	}

	/**
	 * Executes an SQL query on the connected database. Queries using this method
	 * will not return results. Use for INSERT or UPDATE.
	 * 
	 * @param sql SQL query string
	 * @throws SQLException
	 */
	public void executeUpdate(String sql) throws SQLException {
		Statement statement = connection.createStatement();
		statement.executeUpdate(sql);
	}

	/**
	 * Closes connection to database.
	 * 
	 * @throws SQLException
	 */
	public void closeConnection() throws SQLException {
		connection.close();
	}
}
