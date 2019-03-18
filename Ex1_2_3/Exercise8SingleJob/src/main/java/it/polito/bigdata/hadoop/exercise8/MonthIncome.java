package it.polito.bigdata.hadoop.exercise8;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MonthIncome implements org.apache.hadoop.io.Writable {

	private String monthID;
	private double income;

	public String getMonthID() {
		return monthID;
	}

	public void setMonthID(String monthIDValue) {
		monthID = monthIDValue;
	}

	public double getIncome() {
		return income;
	}

	public void setIncome(double incomeValue) {
		income = incomeValue;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		monthID = in.readUTF();
		income = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(monthID);
		out.writeDouble(income);

	}

}
