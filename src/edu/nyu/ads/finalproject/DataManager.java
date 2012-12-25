package edu.nyu.ads.finalproject;

import java.util.ArrayList;
import java.util.List;

public class DataManager {

	protected static List<Site> database;

	protected DataManager() {
		database = new ArrayList<Site>();
		for (int i = 1; i <= 10; i++) {
			Site s = new Site(i);
			database.add(s);
		}
	}

	protected static void generateAuditTrail(Transaction tr) {

		for (Site s : database) {
			for (Variable v : s.getVariablesArray()) {
				if (s.isVariablPresent(v.getVariableID()) /*
														 * &&
														 * s.isVariableReadyToBeRead
														 * (v.getVariableID())
														 */) {
					if (tr.changedVariables.containsKey(v.getVariableID()))
						continue;
					else
						tr.changedVariables.put(v.getVariableID(),
								v.getVariableData());
				}
			}
		}
	}

	protected static List<Integer> getListOfRunningSites() {
		List<Integer> s = new ArrayList<Integer>();
		for (Site site : database) {
			if (site.isUp())
				s.add(site.getSiteNum());
		}
		return s;
	}

	protected static int writeLock(String var, Transaction tr, Lock lock) {
		int num = -1;
		for (Site s : database) {
			if (s.isUp() && s.isVariablPresent(var)) {
				s.lockVar(var, tr, lock);
			}
		}
		return num;
	}

	protected static int readLock(String var, Transaction tr, Lock lock) {
		for (Site s : database) {
			if (s.isUp() && s.isVariablPresent(var)
					&& s.isVariableReadyToBeRead(var)) {
				s.lockVar(var, tr, lock);
				if (lock == Lock.READ || lock == Lock.NONE) {
					return s.getSiteNum();

				}
			}
		}
		return -1;
	}

	protected static Variable readValue(int SiteNum, String var) {
		Variable out;
		Site s = database.get(SiteNum - 1);
		out = s.getVariable(var);
		return out;
	}

	protected static boolean checkForWriteLock(String var) {
		boolean bool = false;
		for (Site site : database) {
			if (site.isUp() && site.isVariablPresent(var)
					&& site.isVariableReadyToBeRead(var)) {
				if (site.isVariableLocked(var)
						&& site.getLockType(var) == Lock.WRITE) {
					bool = true;
					break;
				}
			}
		}
		return bool;
	}

	protected static boolean checkForReadWriteLock(String var) {
		boolean bool = false;
		for (Site site : database) {
			if (site.isUp() && site.isVariablPresent(var)) {
				if (site.isVariableLocked(var)
						&& (site.getLockType(var) == Lock.WRITE || site
								.getLockType(var) == Lock.READ)) {
					bool = true;
					break;
				}
			}
		}
		return bool;
	}

	protected static void dump() {
		for (Site s : database) {
			if (s.isUp()) {
				System.out.println("Site number: " + s.getSiteNum());
				for (int i = 1; i <= 20; i++) {
					String var = "x" + Integer.toString(i);
					if (s.isVariablPresent(var)) {
						Variable variable = readValue(s.getSiteNum(), var);
						System.out.println("Variable: "
								+ variable.getVariableID() + ", Data: "
								+ variable.getVariableData());
					}
				}
			}
		}
	}

	protected static void dump(int site) {
		for (Site s : database) {
			if (s.getSiteNum() == site) {
				if (!s.isUp())
					System.out.println("The site " + site + " is not up.");
				else {
					System.out.println("Site number: " + s.getSiteNum());
					for (int i = 1; i <= 20; i++) {
						String var = "x" + Integer.toString(i);
						if (s.isVariablPresent(var)) {
							Variable variable = readValue(s.getSiteNum(), var);
							System.out.println("Variable: "
									+ variable.getVariableID() + ", Data: "
									+ variable.getVariableData());
						}
					}
				}
			}
		}
	}

	protected static void dump(String var) {
		for (Site s : database) {
			if (s.isUp()) {
				System.out.println("Site number: " + s.getSiteNum());

				if (s.isVariablPresent(var)) {
					Variable variable = readValue(s.getSiteNum(), var);
					System.out.println("Variable: " + variable.getVariableID()
							+ ", Data: " + variable.getVariableData());
				}

			}
		}
	}

	protected static void fail(int site) {
		for (Site s : database) {
			if (s.getSiteNum() == site) {
				if (!s.isUp())
					System.err
							.println("The site " + site + " is already down.");
				else {
					s.failSite();
					s.clearLockTable();
					System.out.println("The site " + site + " is down now.");
				}
				break;
			}
		}

	}

	protected static void recover(int site) {
		for (Site s : database) {
			if (s.getSiteNum() == site) {
				if (s.isUp())
					System.out.println("The site " + site + " is already up.");
				else {
					s.recoverSite();

					System.out.println("The site " + site + " is up now.");
				}
				break;
			}
		}
	}

	protected static Transaction getTransactionHoldingLockOnVariable(String var) {
		Transaction t = null;
		for (Site s : database) {
			if (s.isUp() && s.isVariablPresent(var)) {
				t = s.getTransactionLockingVar(var);
				return t;
			}
		}
		return t;
	}

	protected static void unlock(Transaction tr, int siteNum, String var) {
		for (Site s : database) {
			if (s.getSiteNum() == siteNum) {
				s.unlock(tr, var);
			}
		}
	}

	protected static List<String> listOfTransactionsWithLockOnVariable(
			String var) {
		List<String> list = new ArrayList<String>();
		for (Site s : database) {
			if (s.isUp() && s.isVariablPresent(var)) {
				List<Transaction> temp = new ArrayList<Transaction>(s.getListTransactionLockingVar(var));
				for(Transaction t : temp)
					list.add(t.getTransactionID());
			}
		}
		return list;
	}

	protected static boolean isVariableAvailable(String var) {
		for (Site s : database) {
			if (s.isUp() && s.isVariablPresent(var)
					&& s.isVariableReadyToBeRead(var)) {
				return true;
			} else
				continue;
		}
		return false;
	}

	protected static boolean isVariableAvailableForWrite(String var) {
		for (Site s : database) {
			if (s.isUp() && s.isVariablPresent(var))
				return true;
			else
				continue;
		}
		return false;
	}

	protected static void writeValueToDatabase(String var, int newValue) {
		for (Site s : database) {
			if (s.isUp() && s.isVariablPresent(var)) {
				s.writeValueToSite(var, newValue);
			}
		}
	}

	public static boolean checkForReadLock(String var) {
		boolean bool = false;
		for (Site site : database) {
			if (site.isUp() && site.isVariablPresent(var)
					&& site.isVariableReadyToBeRead(var)) {
				if (site.isVariableLocked(var)
						&& site.getLockType(var) == Lock.READ) {
					bool = true;
					break;
				}
			}
		}
		return bool;
	}

}
