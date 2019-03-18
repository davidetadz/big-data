package it.polito.bigdata.hadoop.exercise13;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData extends
		Mapper<Text, // Input key type
				Text, // Input value type
				NullWritable, // Output key type
				DateIncome> {// Output value type

	private DateIncome top1;

	protected void setup(Context context) {
		// for each mapper, top1 i used to store the information about the top1
		// date-income of the subset of lines analyzed by the mapper
		top1 = new DateIncome();
		top1.setIncome(Float.MIN_VALUE);
		top1.setDate(null);
	}

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		String date = new String(key.toString());

		float dailyIncome = Float.parseFloat(value.toString());

		// Check if the current income is the top 1 income among the ones
		// analyzed by this
		// mapper. In case of tie (same income value) the earliest date is
		// considered.
		if (dailyIncome > top1.getIncome() || 
			(dailyIncome == top1.getIncome() && date.compareTo(top1.getDate()) < 0)) {
			// The current line is the current top 1 income value. Store date
			// and income in top1
			top1 = new DateIncome();
			top1.setDate(date);
			top1.setIncome(dailyIncome);
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Emit the top1 date and income related to this mapper
		context.write(NullWritable.get(), top1);
	}

}
