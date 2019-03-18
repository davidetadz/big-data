package it.polito.bigdata.hadoop.exercise6;

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

		String record = value.toString();

		// Split each record by using the field separator
		// fields[0]= first attribute - sensor id
		// fields[1]= second attribute - date
		// fields[2]= third attribute - reading
		String[] fields = record.split(",");

		// emit the pair (sensor_id, reading value)
		context.write(new Text(fields[0]), new FloatWritable(new Float(fields[2])));
	}
}
