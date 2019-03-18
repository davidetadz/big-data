package it.polito.bigdata.hadoop.exercise8;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 8 - Reducer 2
 */
class ReducerBigDataStep2 extends
		Reducer<Text, // Input key type
				DoubleWritable, // Input value type
				Text, // Output key type
				DoubleWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<DoubleWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		double totalIncome = 0;
		int count = 0;

		// Iterate over the set of values and sum them
		for (DoubleWritable value : values) {
			totalIncome = totalIncome + value.get();
			count++;
		}

		context.write(new Text(key), new DoubleWritable(totalIncome / count));
	}
}
