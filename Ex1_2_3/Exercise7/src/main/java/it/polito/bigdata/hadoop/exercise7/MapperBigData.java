package it.polito.bigdata.hadoop.exercise7;

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
				Text> {// Output value type

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split each sentence in words. Use whitespace(s) as delimiter (=a
		// space, a tab, a line break, or a form feed)
		// The split method returns an array of strings
		String[] words = value.toString().split("\\s+");

		// Iterate over the set of words
		for (String word : words) {
			// Transform word case
			String cleanedWord = word.toLowerCase();

			if (cleanedWord.compareTo("and") != 0 && cleanedWord.compareTo("or") != 0
					&& cleanedWord.compareTo("not") != 0)
				// emit the pair (word, sentenceid)
				context.write(new Text(cleanedWord), new Text(key));
		}
	}
}
