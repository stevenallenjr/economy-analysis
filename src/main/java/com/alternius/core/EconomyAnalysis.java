package com.alternius.core;

import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import com.alternius.db.DatabaseConnector;
import com.alternius.models.Transaction;

/**
 * Main processing class - contains all the logic for querying and updating
 * metrics.
 */
public class EconomyAnalysis {

	private final DatabaseConnector dbConnector;

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
	        updateTotalPerGroup(transaction);
	        insertOrUpdateTransaction(transaction);

	        // If everything was successful, commit the changes
	        dbConnector.commitChanges();
	    } catch (SQLException e) {
	        e.printStackTrace();
	        try {
	            // If there was an exception, roll back any changes made during this transaction
	            dbConnector.rollbackChanges();
	            System.out.println("Transaction rolled back due to an error.");
	        } catch (SQLException ex) {
	            System.out.println("Error rolling back transaction.");
	            ex.printStackTrace();
	        }
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
		double transactionAmount = transaction.getAmount();

		String upsertQuery = "INSERT INTO daily_group_transfer (date, sum_transfers, num_transfers, origin_group_id, destination_group_id) "
				+ "VALUES (?, ?, 1, ?, ?) " + "ON CONFLICT (origin_group_id, destination_group_id, date) "
				+ "DO UPDATE SET sum_transfers = daily_group_transfer.sum_transfers + EXCLUDED.sum_transfers, "
				+ "num_transfers = daily_group_transfer.num_transfers + 1";

		dbConnector.executeUpdate(upsertQuery, transactionDate, transactionAmount, senderGroupId, recipientGroupId);
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
		// Construct query to update the existing record (if it exists)
		String updateQuery = "UPDATE total_by_group SET amount = amount + ? WHERE account_group_id = ? AND date = ?";
		int rowsUpdated = dbConnector.executeUpdate(updateQuery, amountToAdd, groupId, Date.valueOf(transactionDate));

		// If no rows were updated, the record does not exist
		if (rowsUpdated == 0) {
			double initialAmount = calculateInitialAmount(groupId);

			// Insert a new record with the calculated initial amount
			String insertQuery = "INSERT INTO total_by_group (account_group_id, date, amount) VALUES (?, ?, ?)";
			dbConnector.executeUpdate(insertQuery, groupId, Date.valueOf(transactionDate), initialAmount + amountToAdd);
		}
	}

	/**
	 * Calculates the initial daily balance for a group.
	 * 
	 * @param groupId ID of the group to be calculated for
	 * @return initial amount to be assigned to the group
	 * @throws SQLException
	 */
	private double calculateInitialAmount(long groupId) throws SQLException {
		String sumQuery = "SELECT (SELECT SUM(sum_transfers) FROM daily_group_transfer WHERE destination_group_id = ?) - (SELECT SUM(sum_transfers) FROM daily_group_transfer WHERE origin_group_id = ?) AS difference;";
		List<Map<String, Object>> results = dbConnector.executeQuery(sumQuery, groupId, groupId);
		System.out.println(results);
		if (!results.isEmpty() && results.get(0).get("difference") != null) {
			return (double) results.get(0).get("difference");
		}
		return 0;
	}

	/**
	 * Updates the accountTransactions map to store the 20 most recent transactions
	 * for each user. Adds the transaction to JSON and removes the oldest if size >
	 * 20.
	 * 
	 * @param transaction transaction to be processed
	 */
	public void insertOrUpdateTransaction(Transaction transaction) throws SQLException {
		String query = "INSERT INTO recent_transactions (account, date, transactions) "
				+ "VALUES (?, ?, jsonb_build_array(jsonb_build_object('other_account', ?, 'amount', ?, 'is_sender', ?))) "
				+ "ON CONFLICT (account, date) DO UPDATE SET transactions = "
				+ "CASE WHEN jsonb_array_length(recent_transactions.transactions) < 20 THEN recent_transactions.transactions || jsonb_build_object('other_account', ?, 'amount', ?, 'is_sender', ?) "
				+ "ELSE jsonb_build_array() END;";

		long senderAccountId = transaction.getSender().getId();
		LocalDate date = transaction.getDate();
		long recipientAccountId = transaction.getRecipient().getId();
		double amount = transaction.getAmount();

		dbConnector.executeUpdate(query, senderAccountId, date, recipientAccountId, amount, true, recipientAccountId,
				amount, true);
		dbConnector.executeUpdate(query, recipientAccountId, date, senderAccountId, amount, false, senderAccountId,
				amount, false);

	}
}
