package it.polito.bigdata.hadoop.exercise6withcombineranddatatype;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * WordCount Reducer
 */
class CombinerBigData extends Reducer<
                Text,           // Input key type
                MinMaxWritable,  // Input value type
                Text,           // Output key type
                MinMaxWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<MinMaxWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {
    	
    	double min=Double.MAX_VALUE;
    	double max=Double.MIN_VALUE;
        
        // Iterate over the set of values and update min and max.
    	// The format of each input value is min_max
        for (MinMaxWritable value : values) {
			
        	if (value.getMax()>max) {
        		max=value.getMax();
        	}
        	
        	if (value.getMin()<min) {
        		min=value.getMin();
        	}
        }

		// emit the pair (sensor_id, min_max value)
		// value is composed of two parts: min and max.
		MinMaxWritable minMax=new MinMaxWritable();
		
		minMax.setMin(min);
		minMax.setMax(max);
        
        context.write(new Text(key), minMax);
    }
}
