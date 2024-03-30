package com.alternius.core;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;

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
	 * @throws SQLException
	 */
	public void processTransaction(Transaction transaction) throws SQLException {
		try {
			updateGroupTransfers(transaction);
			updateTotalPerGroup(transaction);
			insertOrUpdateTransaction(transaction, transaction.getSender().getId());
			insertOrUpdateTransaction(transaction, transaction.getRecipient().getId());

			// If everything was successful, commit the changes
			dbConnector.connection.commit();
		} catch (SQLException e) {
			try {
				// If there was an exception, roll back any changes made during this transaction
				dbConnector.connection.rollback();
			} catch (SQLException ex) {
				e.addSuppressed(ex);
			}

			throw e;
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
		Timestamp transactionTimestamp = transaction.getTimestamp();
		double transactionAmount = transaction.getAmount();

		String upsertQuery = "INSERT INTO daily_group_transfer (date, sum_transfers, num_transfers, origin_group_id, destination_group_id) "
				+ "VALUES (?, ?, 1, ?, ?) "
				+ "ON CONFLICT (origin_group_id, destination_group_id, date) "
				+ "DO UPDATE SET " + "sum_transfers = daily_group_transfer.sum_transfers + EXCLUDED.sum_transfers, "
				+ "num_transfers = daily_group_transfer.num_transfers + 1;";

		dbConnector.executeUpdate(upsertQuery, transactionTimestamp, transactionAmount, senderGroupId, recipientGroupId);
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
		LocalDate transactionDate = transaction.getTimestamp().toLocalDateTime().toLocalDate();
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
		String updateQuery = "UPDATE total_by_group "
				+ "SET amount = amount + ? "
				+ "WHERE account_group_id = ? "
				+ "AND date = ?;";
		int rowsUpdated = dbConnector.executeUpdate(updateQuery, amountToAdd, groupId, transactionDate);

		// If no rows were updated, the record does not exist
		if (rowsUpdated == 0) {
			double initialAmount = calculateInitialAmount(groupId, transactionDate);

			// Insert a new record with the calculated initial amount
			String insertQuery = "INSERT INTO total_by_group (account_group_id, date, amount) "
					+ "VALUES (?, ?, ?);";
			try (PreparedStatement pstmt = dbConnector.connection.prepareStatement(insertQuery)) {
				pstmt.setLong(1, groupId);
				pstmt.setDate(2, Date.valueOf(transactionDate));
				pstmt.setDouble(3, initialAmount + amountToAdd);
				
				pstmt.executeUpdate();
			}
		}
	}

	/**
	 * Calculates the initial daily balance for a group.
	 * 
	 * @param groupId ID of the group to be calculated for
	 * @return initial amount to be assigned to the group
	 * @throws SQLException
	 */
	private double calculateInitialAmount(long groupId, LocalDate transactionDate) throws SQLException {
		double difference = 0;
		String sumQuery = "SELECT "
				+ "(SELECT SUM(sum_transfers) "
				+ "FROM daily_group_transfer "
				+ "WHERE destination_group_id = ? "
				+ "AND date <= ?) "
				+ "- "
				+ "(SELECT SUM(sum_transfers) "
				+ "FROM daily_group_transfer "
				+ "WHERE origin_group_id = ? "
				+ "AND date <= ?)"
				+ "AS difference;";

		try (PreparedStatement pstmt = dbConnector.connection.prepareStatement(sumQuery)) {
			pstmt.setLong(1, groupId);
			pstmt.setDate(2, Date.valueOf(transactionDate));
			pstmt.setLong(3, groupId);
			pstmt.setDate(4, Date.valueOf(transactionDate));

			try (ResultSet rs = pstmt.executeQuery()) {
				if (rs.next()) {
					difference = rs.getDouble("difference");
				}
			}
		}
		return difference;
	}

	/**
	 * Updates the accountTransactions map to store the 20 most recent transactions
	 * for each user. Adds the transaction to JSON and removes the oldest if size >
	 * 20.
	 * 
	 * @param transaction transaction to be processed
	 */
	public void insertOrUpdateTransaction(Transaction transaction, long accountID) throws SQLException {
		long senderID = transaction.getSender().getId();
		long recipientID = transaction.getRecipient().getId();
		
		String insertQuery = "INSERT INTO recent_transactions (account, timestamp, data) "
				+ "VALUES (?, ?, jsonb_build_object('id', ?, 'amount', ?, 'other_account', ?, 'is_sender', ?));";
		try (PreparedStatement pstmt = dbConnector.connection.prepareStatement(insertQuery)) {
			pstmt.setLong(1, accountID);
			pstmt.setTimestamp(2, transaction.getTimestamp());
			pstmt.setLong(3, transaction.getId());
			pstmt.setDouble(4, transaction.getAmount());
			pstmt.setDouble(5, accountID == senderID ? senderID : recipientID);
			pstmt.setBoolean(6, accountID == senderID);
			
			pstmt.executeUpdate();
		}
		
		// Delete anything past the 20th row
		
		// Set an OFFSET of 20 so that results start at the 20th row, if it exists
		String transactionsQuery = "SELECT timestamp "
				+ "FROM recent_transactions "
				+ "WHERE account = ? "
				+ "ORDER BY timestamp ASC "
				+ "OFFSET 20 "
				+ "LIMIT 1;";
		
		Timestamp deletionStart = null;
		
		try (PreparedStatement pstmt = dbConnector.connection.prepareStatement(transactionsQuery)) {
			pstmt.setLong(1, accountID);
			try (ResultSet rs = pstmt.executeQuery()) {
				if (rs.next()) {
					// Get the timestamp of the 1st row (the 21st row total for the account) and delete all rows at and after this time
					deletionStart = rs.getTimestamp("timestamp");
				}
			}
		}
		
		if (deletionStart != null) {
			String deleteQuery = "DELETE FROM recent_transactions "
					+ "WHERE account = ? "
					+ "AND timestamp >= ?;";
			try (PreparedStatement deleteStmt = dbConnector.connection.prepareStatement(deleteQuery)) {
				deleteStmt.setLong(1, accountID);
				deleteStmt.setTimestamp(2, deletionStart);
				
				deleteStmt.executeUpdate();
			}
		}
	}
}
