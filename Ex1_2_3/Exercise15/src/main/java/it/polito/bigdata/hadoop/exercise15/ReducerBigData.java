package it.polito.bigdata.hadoop.exercise15;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exercise 15 - Reducer
 */
class ReducerBigData extends
		Reducer<Text, // Input key type
				NullWritable, // Input value type
				Text, // Output key type
				IntWritable> { // Output value type

	int wordId;

	protected void setup(Context context) {
		// Initialize the variable that is used to remember how many words have
		// been already mapped to an integer (i.e., it stores also the last
		// integer value mapped with a word)
		wordId = 0;
	}

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<NullWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		// Emit the current word associated with the next available integer
		wordId = wordId + 1;
		context.write(key, new IntWritable(wordId));
	}
}
