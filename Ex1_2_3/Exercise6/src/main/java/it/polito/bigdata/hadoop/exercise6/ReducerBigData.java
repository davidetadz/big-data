package it.polito.bigdata.hadoop.exercise6;

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
				Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<FloatWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;

		// Iterate over the set of values and update min and max.
		for (FloatWritable value : values) {
			if (value.get() > max) {
				max = value.get();
			}

			if (value.get() < min) {
				min = value.get();
			}
		}

		// Emits pair (sensor_id, max_min)
		context.write(new Text(key), new Text("max=" + max + "_min=" + min));
	}
}
