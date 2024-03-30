package com.alternius.models;

import java.sql.Timestamp;
import java.time.LocalDate;

/**
 * Represents a transfer of yuans between two accounts.
 */
public class Transaction {

	private Account sender;
	private Account recipient;

	private double amount;
	private long id;

	private Timestamp timestamp;

	/**
	 * Creates an instance of a Transaction.
	 * 
	 * @param id        long ID of transaction
	 * @param amount    amount of yuans transferred
	 * @param sender    Account of sender
	 * @param recipient Account of recipient
	 * @param date      date the transaction was completed
	 */
	public Transaction(long id, double amount, Account sender, Account recipient, Timestamp date) {
		this.id = id;
		this.amount = amount;
		this.sender = sender;
		this.recipient = recipient;
		this.timestamp = date;
	}

	/**
	 * Returns the sender of the transaction.
	 * 
	 * @return Account sender
	 */
	public Account getSender() {
		return sender;
	}

	/**
	 * Returns the recipient of the transaction.
	 * 
	 * @return Account recipient
	 */
	public Account getRecipient() {
		return recipient;
	}

	/**
	 * Returns the amount of yuans transferred in the transaction.
	 * 
	 * @return double amount
	 */
	public double getAmount() {
		return amount;
	}

	/**
	 * Returns the ID of the transaction.
	 * 
	 * @return long transaction ID
	 */
	public long getId() {
		return id;
	}

	/**
	 * Returns the date the transaction was completed.
	 * 
	 * @return LocalDate of transaction completion
	 */
	public Timestamp getTimestamp() {
		return timestamp;
	}

	/**
	 * Formats transaction into format of `ID | amount | sender ID -> recipient ID`.
	 */
	@Override
	public String toString() {
		return String.format("%d | %f | %d -> %d", id, amount, sender.getId(), recipient.getId());
	}
}
