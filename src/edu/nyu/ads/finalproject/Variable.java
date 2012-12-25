package edu.nyu.ads.finalproject;

public class Variable {

	private String variableID;
	private int variableIndex;
	protected int variableData;
	
	protected Variable(int num) {
		this.variableIndex = num;
		this.variableID = "x" + Integer.toString(num);
		variableData = num*10;
	}

	public String getVariableID() {
		return variableID;
	}

	public int getVariableData() {
		return variableData;
	}

	protected int returnVariableData() {
		return variableData;
	}

	public int getVariableIndex() {
		return variableIndex;
	}

	protected void modifyVariableData(int variableData) {
		this.variableData = variableData;
	}

	protected void setVariableID(String variableID) {
		this.variableID = variableID;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((variableID == null) ? 0 : variableID.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof Variable)) {
			return false;
		}
		Variable other = (Variable) obj;
		if (variableID == null) {
			if (other.variableID != null) {
				return false;
			}
		} else if (!variableID.equals(other.variableID)) {
			return false;
		}
		return true;
	}

	
}
