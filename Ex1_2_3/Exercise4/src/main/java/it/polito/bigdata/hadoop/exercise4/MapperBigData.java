package it.polito.bigdata.hadoop.exercise4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 4 - Mapper
 */
class MapperBigData extends
		Mapper<Text, // Input key type
				Text, // Input value type
				Text, // Output key type
				Text> { // Output value type

	private static Double PM10Threshold = new Double(50);

	protected void map(Text key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Extract zone and date from the key
		String[] fields = key.toString().split(",");

		String zone = fields[0];
		String date = fields[1];
		Double PM10Level = new Double(value.toString());

		// Compare the value of PM10 with the threshold value
		if (PM10Level > PM10Threshold) {
			// emit the pair (sensor_id, 1)
			context.write(new Text(zone), new Text(date));
		}
	}
}
