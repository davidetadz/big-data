package it.polito.bigdata.hadoop.exercise6withcombineranddatatype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxWritable implements org.apache.hadoop.io.Writable
{
	private double min = Double.MAX_VALUE;
	private double max = Double.MIN_VALUE;
	
	public double getMin()
	{
		return min;
	}

	public void setMin(double minValue)
	{
		min=minValue;
	}

	public double getMax()
	{
		return max;
	}

	public void setMax(double maxValue)
	{
		max=maxValue;
	}
	

	@Override
	public void readFields(DataInput in) throws IOException {
		min=in.readDouble();
		max=in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(min);
		out.writeDouble(max);
	}
	
	public String toString()
	{
		String formattedString=new String("max="+max+"_min="+min);
	
		return formattedString;
	}
	
}
