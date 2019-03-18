package it.polito.bigdata.hadoop.exercise6withcombineranddatatype;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Average Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    MinMaxWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
    	
    		String record=value.toString();

            // Split each record by using the field separator
    		// fields[0]= first attribute - sensor id
    		// fields[1]= second attribute - timestamp
			// fields[2]= third attribute - reading
			String[] fields = record.toString().split(",");
				
			// emit the pair (sensor_id, min reading value_max reading value)
			// value is composed of two parts: min and max value (they are the same value in 
			// the mapper).
			MinMaxWritable minMax=new MinMaxWritable();
			
			minMax.setMin(Double.parseDouble(fields[2]));
			minMax.setMax(Double.parseDouble(fields[2]));
				
			context.write(new Text(fields[0]), minMax);
    }
}
