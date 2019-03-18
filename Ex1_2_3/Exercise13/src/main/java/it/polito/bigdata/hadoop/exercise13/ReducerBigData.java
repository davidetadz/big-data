package it.polito.bigdata.hadoop.exercise13;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer
 */
class ReducerBigData extends
		Reducer<NullWritable, // Input key type
				DateIncome, // Input value type
				Text, // Output key type
				FloatWritable> { // Output value type

	// The reduce method is called only once in this approach
	// All the key-value pairs emitted by the mappers have the
	// same key (NullWritable.get())
	@Override
	protected void reduce(NullWritable key, // Input key type
			Iterable<DateIncome> values, // Input value type
			Context context) throws IOException, InterruptedException {

		String date;
		float dailyIncome;

		DateIncome globalTop1 = new DateIncome();
		globalTop1.setIncome(Float.MIN_VALUE);
		globalTop1.setDate(null);

		// Iterate over the set of values and select the top 1 income and
		// the related date
		for (DateIncome value : values) {

			date = value.getDate();
			dailyIncome = value.getIncome();

			if (dailyIncome > globalTop1.getIncome()
				|| (dailyIncome == globalTop1.getIncome() && date.compareTo(globalTop1.getDate()) < 0)) {
				// The current line is the current top 1 income value. Store
				// date and income in globalTop1
				globalTop1 = new DateIncome();
				globalTop1.setDate(date);
				globalTop1.setIncome(dailyIncome);
			}

		}

		// Emit pair (date, income) associated with top 1 income
		context.write(new Text(globalTop1.getDate()), new FloatWritable(globalTop1.getIncome()));
	}
}
