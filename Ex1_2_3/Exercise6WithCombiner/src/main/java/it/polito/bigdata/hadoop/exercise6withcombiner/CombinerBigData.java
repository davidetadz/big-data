package it.polito.bigdata.hadoop.exercise6withcombiner;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner
 */
class CombinerBigData extends
		Reducer<Text, // Input key type
				Text, // Input value type
				Text, // Output key type
				Text> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<Text> values, // Input value type
			Context context) throws IOException, InterruptedException {

		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;

		// Iterate over the set of values and update max and min.
		// The format of each input value is max_min
		for (Text value : values) {
			// fields[0] = max
			// fields[1] = min
			String[] fields = value.toString().split("_");

			if (Double.parseDouble(fields[0]) > max) {
				max = Double.parseDouble(fields[0]);
			}

			if (Double.parseDouble(fields[1]) < min) {
				min = Double.parseDouble(fields[1]);
			}
		}

		// Emit the pair (sensor_id, max reading value_min reading value)
		// value is composed of two parts: max and min.
		context.write(new Text(key), new Text(max + "_" + min));
	}
}
