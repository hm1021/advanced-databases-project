package edu.nyu.ads.finalproject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

public class Transaction {

	private String transactionID;
	private long timestamp;
	private TransactionType type;
	protected Hashtable<String, Integer> changedVariables;
	protected HashMap<Integer ,List<String>> locks;
	private String messageBuffer;

	protected String getMessageBuffer() {
		return messageBuffer;
	}

	protected void setMessageBuffer(String messageBuffer) {
		this.messageBuffer = messageBuffer;
	}

	protected Transaction(String id, TransactionType type) {
		this.transactionID = id;
		this.type = type;
		this.timestamp = System.nanoTime();
		locks = new HashMap<Integer ,List<String>>();
		for(int i : locks.keySet())
			locks.put(i, new ArrayList<String>());
		changedVariables = new Hashtable<String, Integer>();
	}
	
	protected boolean checkIfVariableChanged(String var) {
		return changedVariables.containsKey(var);
	}
	
	protected int getChangedVariable(String var) {
		return changedVariables.get(var);
	}
	
	protected boolean isSiteAccessed(int siteNum) {
		return locks.containsKey(siteNum);
	}
	
	protected void clearChangedVariables() {
		changedVariables.clear();
	}
	
	protected void lockVar(int s, String var) {
		List<String> tmp = locks.get(s);
		if(tmp == null)
			tmp = new ArrayList<String>();
		tmp.add(var);
		locks.put(s, tmp);
	}
	
	protected boolean checkLockOnVar(String var) {
		for(int i : locks.keySet()) {
			if(locks.get(i).contains(var))
				return true;
		}
		return false;
	}
	
	protected int getSiteNumberWithLockOnVariable(String var) {
		
		for(int i : locks.keySet()) {
			if(locks.get(i).contains(var))
				return i;
		}
		return -1;
	}
	
	protected void addToChangedVariables(String varID, int i) {
		changedVariables.put(varID, i);
	}

	protected String getTransactionID() {
		return transactionID;
	}

	protected long getTimestamp() {
		return timestamp;
	}

	protected TransactionType getType() {
		return type;
	}
}
