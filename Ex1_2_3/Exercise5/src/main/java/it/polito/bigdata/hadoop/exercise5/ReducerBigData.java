package it.polito.bigdata.hadoop.exercise5;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
				FloatWritable, // Input value type
				Text, // Output key type
				FloatWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<FloatWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int count = 0;
		double sum = 0;

		// Iterate over the set of values and sum them.
		// Count also the number of values
		for (FloatWritable value : values) {
			sum = sum + value.get();

			count = count + 1;
		}

		// Compute average value
		// Emits pair (sensor_id, average)
		context.write(new Text(key), new FloatWritable((float) sum / count));
	}
}
