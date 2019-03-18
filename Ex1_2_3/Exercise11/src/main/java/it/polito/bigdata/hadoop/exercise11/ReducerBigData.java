package it.polito.bigdata.hadoop.exercise11;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class ReducerBigData extends Reducer<
                Text,           // Input key type
                SumCount,  // Input value type
                Text,           // Output key type
                SumCount> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<SumCount> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	int count=0;
    	float sum=0;
        
        // Iterate over the set of values and sum them.
        // Count also the number of values
        for (SumCount value : values) {
        	sum=sum+value.getSum();
            
        	count=count+value.getCount();
        }

    	SumCount sumCountPerSensor= new SumCount();
    	sumCountPerSensor.setCount(count);
    	sumCountPerSensor.setSum(sum);
    	

        // Emits pair (sensor_id, sum-count = average)
        context.write(new Text(key), sumCountPerSensor);
    }
}
