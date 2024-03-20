package com.alternius.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import com.alternius.db.DatabaseConnector;
import com.alternius.models.Transaction;

/**
 * Main processing class - contains all the logic for querying and updating
 * metrics.
 */
public class EconomyAnalysis {

	private final DatabaseConnector dbConnector;

	// Last 20 transactions for each account - wasn't sure if this was to be stored
	// in memory or in a Postgres database with the other metrics.
	// If I were to store it in a Postgres database, probably store it as a single
	// column using jsonb?
	private Map<Long, LinkedList<Transaction>> accountTransactions = new HashMap<>();

	/**
	 * Constructor for EconomyAnalysis
	 * 
	 * @param dbConnector instance of DatabaseConnector
	 */
	public EconomyAnalysis(DatabaseConnector dbConnector) {
		this.dbConnector = dbConnector;
	}

	/**
	 * Processes details for a given transaction and updates metrics in the
	 * database.
	 * 
	 * @param transaction transaction to be processed
	 */
	public void processTransaction(Transaction transaction) {
		try {
			updateGroupTransfers(transaction);
			updateRecentTransactions(transaction);
			updateTotalPerGroup(transaction);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Calculates and updates metric for total transfers between groups. Does not
	 * track bidirectionally so that, for example, transactions from groups 1 -> 2
	 * can be tracked separately from groups 2 -> 1.
	 * 
	 * @param transaction transaction to be processed
	 * @throws SQLException
	 */
	private void updateGroupTransfers(Transaction transaction) throws SQLException {
		// Get sender ID, recipient ID, and date of transaction
		long senderGroupId = transaction.getSender().getGroupId();
		long recipientGroupId = transaction.getRecipient().getGroupId();
		LocalDate transactionDate = transaction.getDate();

		// Construct query to get sum and number of transfers between sender and
		// recipient groups and date to check whether it exists
		String query = "SELECT sum_transfers, num_transfers FROM daily_group_transfer WHERE origin_group_id = "
				+ senderGroupId + " AND destination_group_id = " + recipientGroupId + " AND date = '" + transactionDate
				+ "'";

		ResultSet rs = dbConnector.executeQuery(query);
		if (rs != null && rs.next()) {
			// Record exists, update it
			double sumTransfers = rs.getDouble("sum_transfers") + transaction.getAmount();
			long numTransfers = rs.getLong("num_transfers") + 1;

			String updateQuery = "UPDATE daily_group_transfer SET sum_transfers = " + sumTransfers
					+ ", num_transfers = " + numTransfers + " WHERE origin_group_id = " + senderGroupId
					+ " AND destination_group_id = " + recipientGroupId + " AND date = '" + transactionDate + "'";
			dbConnector.executeUpdate(updateQuery);
		} else {
			// Record does not exist, insert a new one setting initial sum_transfers value
			// to transaction amount and num_transfers to 1
			String insertQuery = "INSERT INTO daily_group_transfer (date, sum_transfers, num_transfers, origin_group_id, destination_group_id) VALUES ('"
					+ transactionDate + "', " + transaction.getAmount() + ", 1, " + senderGroupId + ", "
					+ recipientGroupId + ")";
			dbConnector.executeUpdate(insertQuery);
		}
	}

	/**
	 * Calculates and updates the total balance per group. For the sender, deducts
	 * the transaction amount from their sum. For the recipient, adds the
	 * transaction amount to their sum.
	 * 
	 * When the first transaction of a day occurs for a given group, calculates the
	 * balance by fetching sum of transfers all-time for the group minus the given
	 * transaction amount. This is not the most efficient approach, but was the best
	 * I could think of with the given data without creating extra tables to store
	 * accounts and balances.
	 * 
	 * @param transaction transaction to be processed
	 * @throws SQLException
	 */
	private void updateTotalPerGroup(Transaction transaction) throws SQLException {
		long senderGroupId = transaction.getSender().getGroupId();
		long recipientGroupId = transaction.getRecipient().getGroupId();
		LocalDate transactionDate = transaction.getDate();
		double transactionAmount = transaction.getAmount();

		// For sender, deduct the transaction amount from their total balance for the
		// date since they sent money
		updateTotalPerDayOrInsert(senderGroupId, transactionDate, -1 * transactionAmount);
		// Opposite for recipient - they received money, so send a positive value to be
		// added
		updateTotalPerDayOrInsert(recipientGroupId, transactionDate, transactionAmount);
	}

	/**
	 * Updates the total balance for a given group by adding the amountToAdd to the
	 * currently stored value.
	 * 
	 * @param groupId         ID of the account_group to be updated
	 * @param transactionDate LocalDate of the transaction
	 * @param amountToAdd     amount to be added to balance, pass negative number to
	 *                        subtract
	 * @throws SQLException
	 */
	private void updateTotalPerDayOrInsert(long groupId, LocalDate transactionDate, double amountToAdd)
			throws SQLException {
		// Construct query to get the balance of the given group on the given date
		String query = "SELECT amount FROM total_by_group WHERE account_group_id = " + groupId + " AND date = '"
				+ transactionDate + "'";

		ResultSet rs = dbConnector.executeQuery(query);
		if (rs != null && rs.next()) {
			// Record exists for the date, update it
			double amount = rs.getDouble("amount");

			String updateQuery = "UPDATE total_by_group SET amount = " + (amount + amountToAdd)
					+ " WHERE account_group_id = " + groupId;
			dbConnector.executeUpdate(updateQuery);
		} else {
			// Record does not exist for their total for the day, so calculate based on sum
			// of received money minus sum of sent money all time
			String sumQuery = "SELECT(SELECT SUM(sum_transfers) FROM daily_group_transfer WHERE destination_group_id = "
					+ groupId + ")" + "-(SELECT SUM(sum_transfers) FROM daily_group_transfer WHERE origin_group_id = "
					+ groupId + ") AS difference;";
			rs = dbConnector.executeQuery(sumQuery);

			if (rs != null && rs.next()) {
				// Fetch difference (sum of received money - sum of sent money all time) and
				// create new row in total_by_group using that as the initial number
				double sum = rs.getDouble("difference");

				String insertQuery = "INSERT INTO total_by_group (account_group_id, date, amount) VALUES (" + groupId
						+ ", '" + transactionDate + "', " + sum + ")";
				dbConnector.executeUpdate(insertQuery);
			}
		}
	}

	/**
	 * Updates the accountTransactions map to store the 20 most recent transactions
	 * for each user. Adds the transaction and removes the oldest if list size > 20.
	 * 
	 * @param transaction transaction to be processed
	 */
	private void updateRecentTransactions(Transaction transaction) {
		// Fetches the transactions list for the sender from the accountTransactions
		// map, or creates it and maps it to the account ID if it does not exist
		LinkedList<Transaction> senderTransactions = accountTransactions
				.computeIfAbsent(transaction.getSender().getId(), id -> new LinkedList<>());
		if (senderTransactions.size() >= 20) {
			// Remove the oldest transaction if the list already has 20 transactions
			senderTransactions.poll();
		}
		senderTransactions.add(transaction);

		// Repeats the logic above for the recipient
		LinkedList<Transaction> recipientTransactions = accountTransactions
				.computeIfAbsent(transaction.getRecipient().getId(), id -> new LinkedList<>());
		if (recipientTransactions.size() >= 20) {
			recipientTransactions.poll();
		}
		recipientTransactions.add(transaction);
	}
}
