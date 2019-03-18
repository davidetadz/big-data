package it.polito.bigdata.hadoop.exercise11;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Average Mapper
 */
class MapperBigData extends
		Mapper<LongWritable, // Input key type
				Text, // Input value type
				Text, // Output key type
				SumCount> {// Output value type

	HashMap<String, SumCount> statistics;

	protected void setup(Context context) {
		statistics = new HashMap<String, SumCount>();
	}

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		SumCount currentStat;

		String record = value.toString();

		// Split each record by using the field separator
		// fields[0]= first attribute - sensor id
		// fields[1]= second attribute - timestamp
		// fields[2]= third attribute - reading
		String[] fields = record.split(",");

		String sensorId = fields[0];
		float measure = Float.parseFloat(fields[2]);

		currentStat = statistics.get(sensorId);

		if (currentStat == null) {
			currentStat = new SumCount();

			currentStat.setCount(1);
			currentStat.setSum(measure);

			statistics.put(new String(sensorId), currentStat);
		} else {
			currentStat.setCount(currentStat.getCount() + 1);
			currentStat.setSum(currentStat.getSum() + measure);

			statistics.put(new String(sensorId), currentStat);
		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {

		for (Entry<String, SumCount> pair : statistics.entrySet()) {
			context.write(new Text(pair.getKey()), pair.getValue());
		}
	}

}
