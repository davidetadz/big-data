package it.polito.bigdata.hadoop.exercise12;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper
 */
class MapperBigData extends
		Mapper<Text, // Input key type
				Text, // Input value type
				Text, // Output key type
				FloatWritable> {// Output value type

	float threshold;

	protected void setup(Context context) {
		// I retrieve the value of the threshold only one time for each mapper
		threshold = Float.parseFloat(context.getConfiguration().get("maxThreshold"));
	}

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		float measure;

		measure = Float.parseFloat(value.toString());

		// Filter the reading based on the value of threshold
		if (measure < threshold) {
			context.write(new Text(key), new FloatWritable(measure));
		}

	}

}
