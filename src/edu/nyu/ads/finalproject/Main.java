package edu.nyu.ads.finalproject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

public class Main {

	public static void main(String[] args) throws Exception {
		TransactionManager tm = new TransactionManager();
		new DataManager();
		System.out.println("Please enter the absolute path of the test file: ");
		Scanner filename = new Scanner(System.in);
		File in = new File(filename.nextLine());
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(in));
			String line = null;
			while ((line = br.readLine()) != null) {
				line = line.replace(" ", "");
				String[] actions = line.split(";");
				for (String a : actions) {
					String[] lineSplit = a.split("\\(");
					String action = lineSplit[0];
					String transactionID = null;
					String[] variables;
					String variableID;
					int siteNum;
					Transaction tr;
					switch (action) {
					case "begin":
						transactionID = lineSplit[1].split("\\)")[0];
						tm.begin(transactionID, TransactionType.RW);
						break;
					case "beginRO":
						transactionID = lineSplit[1].split("\\)")[0];
						tm.begin(transactionID, TransactionType.RO);
						break;
					case "W":
						variables = lineSplit[1].split("\\)");
						transactionID = variables[0].split(",")[0];
						variableID = variables[0].split(",")[1];
						tr = tm.getTransaction(transactionID);
						if (tr == null) {
							System.err.println("Transaction " + transactionID
									+ ": does not exist.");
							break;
						}
						if(tr.getType() == TransactionType.RO) {
							System.err.println("Transaction " + transactionID
									+ ": is of type Read Only");
							break;
						}
						int newValue = Integer
								.parseInt(variables[0].split(",")[2]);
						tm.write(tr, variableID, newValue);
						break;
					case "R":
						variables = lineSplit[1].split("\\)");
						transactionID = variables[0].split(",")[0];
						variableID = variables[0].split(",")[1];
						tr = tm.getTransaction(transactionID);
						if (tr == null) {
							System.err.println("Transaction " + transactionID
									+ ": does not exist.");
							break;
						}
						tm.read(tr, variableID);
						break;
					case "dump":
						variables = lineSplit[1].split("\\)");
						if (variables.length == 0)
							tm.dump();
						else {
							if (variables[0].matches("x.*")) {
								tm.dump(variables[0]);
							}
							else if (variables[0].matches("[0-9]0*")) {
								int siteNumber = Integer.parseInt(variables[0]);
								if (siteNumber == 0 || siteNumber > 10) {
									System.err.println(
											"Site numbers range from 1 to 10.");
									break;
								}
								tm.dump(siteNumber);
							}
						}
						break;
					case "end":
						transactionID = lineSplit[1].split("\\)")[0];
						tr = tm.getTransaction(transactionID);
						if (tr == null) {
							System.err.println("Transaction " + transactionID
									+ ": does not exist.");
							break;
						}
						tm.end(tr);
						break;
					case "fail":
						variables = lineSplit[1].split("\\)");
						siteNum = Integer.parseInt(variables[0]);
						if (siteNum == 0 || siteNum > 10) {
							System.err.println(
									"Site numbers range from 1 to 10.");
							break;
						}
						tm.fail(siteNum);
						break;
					case "recover":
						variables = lineSplit[1].split("\\)");
						siteNum = Integer.parseInt(variables[0]);
						if (siteNum == 0 || siteNum > 10) {
							System.err.println(
									"Site numbers range from 1 to 10.");
							break;
						}
						tm.recover(siteNum);
						tm.dequeueWaitingTransactions();
						break;
					default:
						System.err.println("Invalid operation in test file");
						break;
					}
				}

			}
			br.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
	}
}
