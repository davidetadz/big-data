package it.polito.bigdata.hadoop.exercise8;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 8 - Mapper 2
 */
class MapperBigDataStep2 extends
		Mapper<Text, // Input key type
				Text, // Input value type
				Text, // Output key type
				DoubleWritable> {// Output value type

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		String[] month = key.toString().split("-");

		String year = new String(month[0]);

		// emit the pair (month, value)
		context.write(new Text(year), new DoubleWritable(Double.parseDouble(value.toString())));
	}
}
