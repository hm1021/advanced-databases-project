package edu.nyu.ads.finalproject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class Site {

	private int siteNum;
	private HashMap<Variable, Boolean> variable;
	private boolean isRunning;
	private HashMap<Variable, HashMap<Transaction, Lock>> locktable;
	private long timestamp;
	private boolean isRecovered;
	private HashMap<String, Boolean> isReady;

	protected Site(int siteNumber) {
		this.siteNum = siteNumber;
		variable = new HashMap<Variable, Boolean>();
		locktable = new HashMap<Variable, HashMap<Transaction, Lock>>();
		isReady = new HashMap<String, Boolean>();
		for (int i = 1; i <= 20; i++) {
			Variable v = new Variable(i);
			if (i % 2 == 0) {
				variable.put(v, true);
				isReady.put(v.getVariableID(), true);
			} else if ((i + 1) % 10 == siteNum) {
				variable.put(v, true);
				isReady.put(v.getVariableID(), true);
			} else {
				variable.put(v, false);
				isReady.put(v.getVariableID(), false);
			}
			locktable.put(v, new HashMap<Transaction, Lock>());
		}

		isRunning = true;
		timestamp = System.currentTimeMillis();
		isRecovered = true;
	}

	protected boolean isVariableReadyToBeRead(String var) {
		return isReady.get(var);
	}

	protected boolean isVariablPresent(String var) {
		for (Variable v : variable.keySet()) {
			if (var.equals(v.getVariableID())) {
				boolean b = variable.get(v);
				return b;
			}
		}
		return false;
	}

	protected boolean isVariableLocked(String var) {
		boolean bool = false;
		for (Variable v : locktable.keySet()) {
			if (var.equals(v.getVariableID()))
				if (locktable.get(v) != null)
					bool = true;
		}
		return bool;
	}

	protected boolean lockVar(String var, Transaction tr, Lock lock) {
		if (tr.getType() == TransactionType.RO)
			return true;
		for (Variable v : locktable.keySet()) {
			if (var.equals(v.getVariableID())) {
				locktable.get(v).put(tr, lock);
				return true;
			}
		}

		return false;
	}

	protected long getTimestamp() {
		return timestamp;
	}

	protected int getVariableData(String var) {
		int out = -1;
		for (Variable v : variable.keySet()) {
			if (var.equals(v.getVariableID()) && variable.get(v)) {
				out = v.getVariableData();
			}
		}
		return out;
	}

	protected Lock getLockType(String var) {
		Lock lock = Lock.NONE;
		for (Variable v : locktable.keySet()) {
			if (var.equals(v.getVariableID()) && variable.get(v)) {
				if (!locktable.get(v).isEmpty()) {
					Iterator<Lock> locks = locktable.get(v).values().iterator();
					while (locks.hasNext()) {
						Lock tmp = locks.next();
						if (tmp == Lock.WRITE || lock == Lock.NONE)
							lock = tmp;
					}
				}
			}
		}
		return lock;
	}

	public Variable[] getVariablesArray() {
		return variable.keySet().toArray(new Variable[20]);
	}

	protected Variable getVariable(String varName) {
		for (Variable v : variable.keySet()) {
			if (varName.equals(v.getVariableID()))
				return v;
		}
		return null;
	}

	public int getSiteNum() {
		return siteNum;
	}

	protected void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	protected boolean isUp() {
		return isRunning;
	}

	protected boolean isRecovered() {
		return isRecovered;
	}

	protected void failSite() {
		isRunning = false;
		isRecovered = false;
		for(String var : isReady.keySet()) {
			isReady.put(var, false);
		}
	}

	protected void recoverSite() {
		timestamp = System.currentTimeMillis();
		isRunning = true;
		for (Variable v : variable.keySet()) {
			locktable.put(v, new HashMap<Transaction, Lock>());
		}
		for(String var : isReady.keySet()) {
			if(isVariablPresent(var) && !isReplicated(var))
				isReady.put(var, true);
			else
				isReady.put(var, false);
		}
	}

	protected void clearLockTable() {
		locktable.clear();
	}

	protected Transaction getTransactionLockingVar(String var) {
		for (Variable v : locktable.keySet()) {
			if (var.equals(v.getVariableID())) {
				Iterator<Transaction> iterator = locktable.get(v).keySet()
						.iterator();
				if (iterator.hasNext()) {
					return iterator.next();
				}
			}
		}
		return null;
	}
	
	protected List<Transaction> getListTransactionLockingVar(String var) {
		List<Transaction> list = new ArrayList<Transaction>();
		for (Variable v : locktable.keySet()) {
			if (var.equals(v.getVariableID())) {
				Iterator<Transaction> iterator = locktable.get(v).keySet()
						.iterator();
				if (iterator.hasNext()) {
					list.add(iterator.next());
				}
			}
		}
		return list;
	}

	protected void unlock(Transaction tr, String var) {
		for (Variable v : locktable.keySet()) {
			if (var.equals(v.getVariableID())) {
				locktable.get(v).remove(tr);
			}
		}
	}

	protected void writeValueToSite(String var, int newValue) {
		Variable tmp = null;
		for (Variable v : variable.keySet()) {
			if (var.equals(v.getVariableID())) {
				tmp = v;
				v.variableData = newValue;
			}
		}
		variable.put(tmp, true);
		if(!isVariableReadyToBeRead(var)) {
			isReady.put(var, true);
		}
	}
	
	protected boolean isReplicated(String v) {
		boolean bool = false;
		int num = Integer.parseInt(v.substring(1));
		if(num%2 == 0)
			bool = true;
		
		return bool;
	}
}
