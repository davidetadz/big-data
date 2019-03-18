package it.polito.bigdata.hadoop.exercise5withcombiner;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Combiner Reducer
 */
class CombinerBigData extends
		Reducer<Text, // Input key type
				StatisticsWritable, // Input value type
				Text, // Output key type
				StatisticsWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<StatisticsWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		int localCount = 0;
		float localSum = 0;

		// Iterate over the set of values and sum them.
		// Sum also the "number of values"
		for (StatisticsWritable value : values) {
			localSum = localSum + value.getSum();
			localCount = localCount + value.getCount();
		}

		StatisticsWritable localSumAndCount = new StatisticsWritable();
		localSumAndCount.setCount(localCount);
		localSumAndCount.setSum(localSum);

		// Emits pair (sensor_id, sum values - sum counts)
		context.write(new Text(key), localSumAndCount);
	}
}
