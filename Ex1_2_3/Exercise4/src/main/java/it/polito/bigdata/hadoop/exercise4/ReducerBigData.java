package it.polito.bigdata.hadoop.exercise4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 4 - Reducer
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
				Text, // Input value type
				Text, // Output key type
				Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		String aboveThresholdDates = new String();

		// Iterate over the set of values and concatenate them
		for (Text date : values) {
			if (aboveThresholdDates.length() == 0)
				aboveThresholdDates = new String(date.toString());
			else
				aboveThresholdDates = aboveThresholdDates.concat("," + date.toString());
		}

		context.write(new Text(key), new Text(aboveThresholdDates));
	}
}
