package it.polito.bigdata.hadoop.exercise5;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Average Mapper
 */
class MapperBigData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				Text, // Output key type
				FloatWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Split each record by using the field separator
		// fields[0]= first attribute - sensor id
		// fields[1]= second attribute - date
		// fields[2]= third attribute - PM10 value
		String[] fields = value.toString().split(",");
		String sensorId = fields[0];
		float PM10value = Float.parseFloat(fields[2]);

		// emit the pair (sensor_id, reading value)
		context.write(new Text(sensorId), new FloatWritable(new Float(PM10value)));
	}
}
