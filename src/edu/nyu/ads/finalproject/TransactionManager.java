package edu.nyu.ads.finalproject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class TransactionManager {

	private List<Transaction> activeTransactions;
	private static LinkedHashMap<Transaction, String> waitingTransactions;
	private List<Transaction> abortTransactions;

	protected TransactionManager() {
		activeTransactions = new ArrayList<Transaction>();
		waitingTransactions = new LinkedHashMap<Transaction, String>();
		abortTransactions = new ArrayList<Transaction>();
	}

	protected Transaction begin(String id, TransactionType type) {
		Transaction tr = new Transaction(id, type);
		activeTransactions.add(tr);
		if (tr.getType() == TransactionType.RO) {
			DataManager.generateAuditTrail(tr);
		}
		System.out.println("Transaction " + tr.getTransactionID() + " of type "
				+ type + " has initialized.");
		return tr;
	}

	protected void end(Transaction tr) {
		if (tr.getType() == TransactionType.RW) {
			if (!tr.changedVariables.isEmpty()) {
				for (String variable : tr.changedVariables.keySet()) {
					int newValue = tr.changedVariables.get(variable);
					DataManager.writeValueToDatabase(variable, newValue);
					System.out.println("Transaction " + tr.getTransactionID()
							+ ": changed the value of " + variable + " to "
							+ newValue + " in database.");
				}
			}

		}
		System.out.println("Transaction " + tr.getTransactionID()
				+ " has ended.");
		kill(tr);
	}

	protected void read(Transaction tr, String var) {
		if (DataManager.isVariableAvailable(var)) {
			if (tr.getType() == TransactionType.RO) {
				for (String v : tr.changedVariables.keySet()) {
					if (var.equals(v)) {
						int siteNum = DataManager.readLock(var, tr, Lock.NONE);
						System.out.println("Transaction "
								+ tr.getTransactionID()
								+ ": Reading the variable " + v + "." + siteNum
								+ ", The value is "
								+ tr.changedVariables.get(v));
						return;
					}
				}

			} else if (tr.getType() == TransactionType.RW) {
				if (tr.checkLockOnVar(var)) {
					List<Integer> runningSites = DataManager
							.getListOfRunningSites();
					int siteNum = tr.getSiteNumberWithLockOnVariable(var);
					if (runningSites.contains(siteNum)) {
						int variableData;
						if (tr.checkIfVariableChanged(var))
							variableData = tr.getChangedVariable(var);
						else {
							Variable v = DataManager.readValue(siteNum, var);
							variableData = v.getVariableData();
						}
						System.out.println("Transaction "
								+ tr.getTransactionID()
								+ ": Reading the variable " + var + "."
								+ siteNum + ", The value is " + variableData);
						return;
					}
				}
				boolean b = DataManager.checkForWriteLock(var);
				boolean checkForReadLock = DataManager.checkForReadLock(var);
				boolean anyOlderTransactionInQueue = false;
				if (checkForReadLock == true) {
					anyOlderTransactionInQueue = checkOlderTransactionInWaitingQueue(var);
				}
				if (b == false && !anyOlderTransactionInQueue) {
					int siteNum = DataManager.readLock(var, tr, Lock.READ);
					tr.lockVar(siteNum, var);
					Variable out = DataManager.readValue(siteNum, var);
					System.out.println("Transaction " + tr.getTransactionID()
							+ ": Reading the variable " + out.getVariableID()
							+ "." + siteNum + ", The value is "
							+ out.getVariableData());
					return;

				} else {
					Transaction t = DataManager
							.getTransactionHoldingLockOnVariable(var);
					boolean bool = youngerTransaction(tr, t);
					if (bool == true) {
						kill(tr);
						System.out
								.println("Transaction "
										+ tr.getTransactionID()
										+ ": has been aborted as an older transaction with TransactionID "
										+ t.getTransactionID()
										+ " is holding a lock on the variable "
										+ var);
					} else {
						boolean isOlderTransactionPresentInQueue = checkForOlderTransactionsInWaitingQueue(
								tr, var);
						if (isOlderTransactionPresentInQueue == true) {
							kill(tr);
							System.out
									.println("Transaction "
											+ tr.getTransactionID()
											+ ": has been aborted as an older transaction is waiting in the queue");
						} else {
							addToWaitingQueue(tr, var);
							tr.setMessageBuffer("read,");
							System.out.println("Transaction "
									+ tr.getTransactionID()
									+ ": has been added to the waiting queue.");
						}
					}
				}
			}
		} else {
			addToWaitingQueue(tr, var);
			tr.setMessageBuffer("read,");
			System.out
					.println("Transaction "
							+ tr.getTransactionID()
							+ ": added to the waiting queue due to variable not available to be read");
			return;
		}
	}

	private boolean checkOlderTransactionInWaitingQueue(String var) {
		for (Transaction t : waitingTransactions.keySet()) {
			if (t.getType() != TransactionType.RO) {
				if (var.equals(waitingTransactions.get(t))) {
					return true;
				}
			}
		}
		return false;
	}

	private boolean checkForOlderTransactionsInWaitingQueue(Transaction tr,
			String var) {
		boolean b = false;
		for (Transaction t : waitingTransactions.keySet()) {
			if (t.getType() == TransactionType.RW
					&& var.equals(waitingTransactions.get(t))) {
				b = tr.getTimestamp() > t.getTimestamp();
				if (b == true)
					return b;
			}
		}
		return false;
	}

	private void kill(Transaction tr) {
		if (activeTransactions.contains(tr)) {
			activeTransactions.remove(tr);
		}

		if (waitingTransactions.containsKey(tr)) {
			waitingTransactions.remove(tr);
		}
		tr.clearChangedVariables();
		for (int siteNum : tr.locks.keySet()) {
			for (String s : tr.locks.get(siteNum)) {
				DataManager.unlock(tr, siteNum, s);
			}
		}
		dequeueWaitingTransactions();
	}

	private boolean youngerTransaction(Transaction tr, Transaction t) {
		if(t == null)
			return false;
		if (tr.getTimestamp() < t.getTimestamp()) {
			return false;
		} else
			return true;
	}

	protected boolean write(Transaction tr, String var, int value) {
		boolean bool = false;
		if (DataManager.isVariableAvailableForWrite(var)) {

			boolean b = DataManager.checkForReadWriteLock(var);
			boolean isUpgradePossible = checkForUpgrade(tr.getTransactionID(),var);
			if (b == false || isUpgradePossible) {
				DataManager.writeLock(var, tr, Lock.WRITE);
				for (int i = 1; i <= 10; i++)
					tr.lockVar(i, var);
				tr.addToChangedVariables(var, value);
				System.out.println("Transaction " + tr.getTransactionID()
						+ ": changed the value of " + var + " to " + value
						+ " in local copy");
				bool = true;
			} else {
				Transaction t = DataManager
						.getTransactionHoldingLockOnVariable(var);
				boolean isTransactionYounger = youngerTransaction(tr, t);
				if (isTransactionYounger == true) {
					kill(tr);
					System.out
							.println("Transaction "
									+ tr.getTransactionID()
									+ ": has been aborted as an older transaction with TransactionID "
									+ t.getTransactionID()
									+ " is holding a lock on the variable "
									+ var);
				} else {
					boolean isOlderTransactionPresentInQueue = checkForOlderTransactionsInWaitingQueue(
							tr, var);
					if (isOlderTransactionPresentInQueue == true) {
						kill(tr);
						System.out
								.println("Transaction "
										+ tr.getTransactionID()
										+ ": has been aborted as an older transaction is waiting in the queue");
					} else {
						addToWaitingQueue(tr, var);
						tr.setMessageBuffer("write," + value);
						System.out.println("Transaction "
								+ tr.getTransactionID()
								+ ": has been added to the waiting queue.");
					}
				}
			}
		} else {
			addToWaitingQueue(tr, var);
			tr.setMessageBuffer("write," + value);
			System.out.println("Transaction " + tr.getTransactionID()
					+ ": has been added to the waiting queue due to variable"
					+ var + " not being available to write.");
		}
		return bool;
	}

	private boolean checkForUpgrade(String transactionID, String var) {
		int count = 0;
		boolean b = false;
		List<String> list = new ArrayList<String>(DataManager.listOfTransactionsWithLockOnVariable(var));
		for(String s : list) {
			if(transactionID.equals(s)) {
				b = true;
			} else {
				count++;
			}
		}
		if(b==true && count == 0)
			return true;
		else
			return false;
	}

	protected void dequeueWaitingTransactions() {
		Set<Transaction> set = new LinkedHashSet<Transaction>(
				waitingTransactions.keySet());
		for (Transaction t : set) {
			String var = waitingTransactions.get(t);
			waitingTransactions.remove(t);
			if (t.getType() == TransactionType.RO) {
				read(t, var);
			} else {
				String[] operation = t.getMessageBuffer().split(",");
				switch (operation[0]) {
				case "read":
					read(t, var);
					break;
				case "write":
					int value = Integer.parseInt(operation[1]);
					write(t, var, value);
					break;
				}
			}
		}
	}

	protected static void addToWaitingQueue(Transaction tr, String var) {
		if (waitingTransactions.containsKey(tr))
			return;
		else
			waitingTransactions.put(tr, var);
	}

	protected Transaction getTransaction(String transactionID) {
		for (Transaction tr : activeTransactions) {
			if (tr.getTransactionID().equals(transactionID))
				return tr;
		}
		return null;
	}

	protected void dump() {
		DataManager.dump();
	}

	protected void dump(int site) {
		DataManager.dump(site);
	}

	protected void dump(String var) {
		DataManager.dump(var);
	}

	protected void fail(int siteNum) {
		DataManager.fail(siteNum);
		Set<Transaction> set = new LinkedHashSet<Transaction>(
				activeTransactions);
		for (Transaction t : set) {
			if (t.isSiteAccessed(siteNum)) {
				abortTransactions.add(t);
				System.out.println("Transaction " + t.getTransactionID()
						+ ": has aborted as the site " + siteNum
						+ " accessed by it, has failed.");
				kill(t);
			}
		}
	}

	protected void recover(int siteNum) {
		DataManager.recover(siteNum);
	}

	// protected void queryState() {
	//
	// }

}
