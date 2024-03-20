package com.alternius.test;

import com.alternius.db.DatabaseConnector;
import com.alternius.models.Account;
import com.alternius.models.Transaction;

import java.sql.SQLException;
import java.time.LocalDate;

import com.alternius.core.EconomyAnalysis;

/**
 * Test class to process mock transactions.
 */
public class TransactionWorker {

	// Database connection information - obscured even though it's from a locally
	// hosted server. But I have principles. Avoiding hardcoding credentials clearly
	// is not one, though.
	private final static String DB_HOST = "xxx";
	private final static String DB_PORT = "xxx";
	private final static String DB_USER = "xxx";
	private final static String DB_PASSWORD = "xxx";
	private final static String DB_DATABASE = "xxx";

	private static Account[] mockAccounts = { new Account(1000000000000001L, 1), new Account(2000000000000001L, 2),
			new Account(3000000000000001L, 3), new Account(4000000000000001L, 4), new Account(1000000000000002L, 1), };

	public static void main(String[] args) {
		try {
			DatabaseConnector dbConnector = new DatabaseConnector(DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_DATABASE);

			EconomyAnalysis economyAnalysis = new EconomyAnalysis(dbConnector);

			// Creates 50 random transactions and processes each one
			for (int i = 0; i < 50; i++) {
				// Picks a random account from mockAccounts to be the sender
				Account sender = mockAccounts[(int) (Math.random() * mockAccounts.length)];
				// Picks a random account from mockAccounts to be the recipient, but ensures it
				// is not the same account as the sender
				Account recipient = mockAccounts[(int) (Math.random() * mockAccounts.length)];
				while (recipient.getId() == sender.getId()) {
					recipient = mockAccounts[(int) (Math.random() * mockAccounts.length)];
				}

				// Creates instance of Transaction using mock data, including a random ID and
				// amount
				Transaction mockTransaction = new Transaction(getRandomLong(), getRandomAmount(), sender, recipient,
						LocalDate.now());
				// Debugging - prints each created transaction to console
				System.out.println(mockTransaction + "\n");
				// Processes metrics using transaction
				economyAnalysis.processTransaction(mockTransaction);
			}
			// Welcome to lazy exception handling
		} catch (ClassNotFoundException e) {
			System.err.println("PostgreSQL driver not found");
		} catch (SQLException e) {
			System.err.println("Invalid database details");
			e.printStackTrace();
		}
	}

	private static long getRandomLong() {
		// Keeping to 16 digits for readability's sake
		long min = 1000000000000000L;
		long max = 9999999999999999L;
		return min + (long) (Math.random() * (max - min));
	}

	private static long getRandomAmount() {
		// Keeping amounts low also for readability's sake
		long min = 1000L;
		long max = 100000L;
		return min + (long) (Math.random() * (max - min));
	}
}