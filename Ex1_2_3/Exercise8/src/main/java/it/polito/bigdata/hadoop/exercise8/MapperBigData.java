package it.polito.bigdata.hadoop.exercise8;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 8 - Mapper
 */
class MapperBigData extends
		Mapper<Text, // Input key type
				Text, // Input value type
				Text, // Output key type
				DoubleWritable> {// Output value type

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		String[] date = key.toString().split("-");

		String month = new String(date[0] + "-" + date[1]);

		// emit the pair (month, value)
		context.write(new Text(month), new DoubleWritable(Double.parseDouble(value.toString())));
	}
}
