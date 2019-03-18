package it.polito.bigdata.hadoop.exercise8;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 8 - Reducer
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
				MonthIncome, // Input value type
				Text, // Output key type
				DoubleWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<MonthIncome> values, // Input value type
			Context context) throws IOException, InterruptedException {

		// NOTE: Local HashMap used to store total income for each month
		HashMap<String, Double> totalMonthIncome = new HashMap<String, Double>();

		String year = key.toString();

		double totalYearlyIncome = 0;
		int countMonths = 0;

		// Iterate over the set of values and compute
		// - the total income for each month
		// - the overall total income for this year
		for (MonthIncome value : values) {
			Double income = totalMonthIncome.get(value.getMonthID());

			if (income != null) {
				// Update the total income for this month
				totalMonthIncome.put(new String(value.getMonthID()), new Double(value.getIncome() + income));
			} else {
				// First occurrence of this monthId
				// Insert monthid - income in the hashmap
				totalMonthIncome.put(new String(value.getMonthID()), new Double(value.getIncome()));

				// Update the number of months of the current year
				countMonths++;
			}

			// Update the total income of the current year
			totalYearlyIncome = totalYearlyIncome + value.getIncome();
		}

		// Emit the pairs (year-month, total monthly income)
		for (Entry<String, Double> pair : totalMonthIncome.entrySet()) {
			context.write(new Text(year + "-" + pair.getKey()), new DoubleWritable(pair.getValue()));
		}

		// Emit the average monthly income for each year
		context.write(new Text(year), new DoubleWritable(totalYearlyIncome / countMonths));

	}
}