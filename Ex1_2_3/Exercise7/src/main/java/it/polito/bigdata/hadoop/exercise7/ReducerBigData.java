package it.polito.bigdata.hadoop.exercise7;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
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

		String invIndex = new String();

		// Iterate over the set of sentenceids and concatenate them
		for (Text value : values) {
			invIndex = invIndex.concat(value + ",");
		}
		context.write(key, new Text(invIndex));
	}
}
