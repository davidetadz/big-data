package it.polito.bigdata.hadoop.exercise9;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 9 - Mapper
 */
class MapperBigData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				Text, // Output key type
				IntWritable> {// Output value type

	HashMap<String, Integer> wordsCounts;

	protected void setup(Context context) {
		wordsCounts = new HashMap<String, Integer>();
	}

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		Integer currentFreq;
		// Split each sentence in words. Use whitespace(s) as delimiter (=a
		// space, a tab, a line break, or a form feed)
		// The split method returns an array of strings
		String[] words = value.toString().split("\\s+");

		// Iterate over the set of words
		for (String word : words) {
			// Transform word case
			String cleanedWord = word.toLowerCase();

			currentFreq = wordsCounts.get(cleanedWord);

			if (currentFreq == null) { // it is the first time that the mapper
										// finds this word
				wordsCounts.put(new String(cleanedWord), new Integer(1));
			} else { // Increase the number of occurrences of the current word
				currentFreq = currentFreq + 1;
				wordsCounts.put(new String(cleanedWord), new Integer(currentFreq));
			}
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {

		// Emit the set of (key, value) pairs of this mapper
		for (Entry<String, Integer> pair : wordsCounts.entrySet()) {
			context.write(new Text(pair.getKey()), new IntWritable(pair.getValue()));
		}

	}
}
