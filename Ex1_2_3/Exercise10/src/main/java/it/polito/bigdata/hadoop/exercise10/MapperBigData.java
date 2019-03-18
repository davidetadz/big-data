package it.polito.bigdata.hadoop.exercise10;

import it.polito.bigdata.hadoop.exercise10.DriverBigData.MY_COUNTERS;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Average Mapper
 */
class MapperBigData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				NullWritable, // Output key type
				NullWritable> {// Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		context.getCounter(MY_COUNTERS.TOTAL_RECORDS).increment(1);

	}
}
