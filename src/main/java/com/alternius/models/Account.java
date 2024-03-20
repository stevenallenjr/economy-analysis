package com.alternius.models;

/**
 * An account for the economy. This can be a player account or a server account.
 */
public class Account {

	private long id;
	private long groupId;

	/**
	 * Creates an instance of an Account.
	 * 
	 * @param id      long ID of account
	 * @param groupId long ID of group from account_group in database
	 */
	public Account(long id, long groupId) {
		this.id = id;
		this.groupId = groupId;
	}

	/**
	 * Getter for account ID
	 * 
	 * @return account ID
	 */
	public long getId() {
		return id;
	}

	/**
	 * Getter for group ID
	 * 
	 * @return group ID
	 */
	public long getGroupId() {
		return groupId;
	}

}
