package it.polito.bigdata.hadoop.exercise3;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exercise 3 - Mapper
 */
class MapperBigData extends Mapper<
                    Text, 		  // Input key type
                    Text, 		  // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
	private static Double PM10Threshold = new Double(50);
	
    protected void map(
            Text key,   		// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Extract sensor and date from the key
            String[] fields = key.toString().split(",");
            
            String sensor_id=fields[0];
            Double PM10Level=new Double(value.toString());
            
            // Compare the value of PM10 with the threshold value
            if (PM10Level>PM10Threshold)
            {
                // emit the pair (sensor_id, 1)
                context.write(new Text(sensor_id), new IntWritable(1));
            }
    }
}
