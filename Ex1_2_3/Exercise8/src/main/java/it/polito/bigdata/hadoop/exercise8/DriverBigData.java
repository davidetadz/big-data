package it.polito.bigdata.hadoop.exercise8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * MapReduce program
 */
public class DriverBigData extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		Path inputPath;
		Path outputDir;
		Path outputDirStep2;
		int numberOfReducers;
		int exitCode;

		// Parse the parameters
		numberOfReducers = Integer.parseInt(args[0]);
		inputPath = new Path(args[1]);
		outputDir = new Path(args[2]);
		outputDirStep2 = new Path(args[3]);

		Configuration conf = this.getConf();

		// First job

		// Define a new job
		Job job = Job.getInstance(conf);

		// Assign a name to the job
		job.setJobName("Exercise #8");

		// Set path of the input file/folder (if it is a folder, the job reads
		// all the files in the specified folder) for this job
		FileInputFormat.addInputPath(job, inputPath);

		// Set path of the output folder for this job
		FileOutputFormat.setOutputPath(job, outputDir);

		// Specify the class of the Driver for this job
		job.setJarByClass(DriverBigData.class);

		// Set job input format
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// Set job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set map class
		job.setMapperClass(MapperBigData.class);

		// Set map output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		// Set reduce class
		job.setReducerClass(ReducerBigData.class);

		// Set reduce output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// Set number of reducers
		job.setNumReduceTasks(numberOfReducers);

		// Execute the job and wait for completion
		if (job.waitForCompletion(true) == true)
			exitCode = 0;
		else
			exitCode = 1;

		if (exitCode == 0) {

			// Second job

			// Define a new job
			Job job2 = Job.getInstance(conf);

			// Assign a name to the job
			job2.setJobName("Exercise #8 - step 2");

			// Set path of the input file/folder (if it is a folder, the job
			// reads all the files in the specified folder) for this job
			FileInputFormat.addInputPath(job2, outputDir);

			// Set path of the output folder for this job
			FileOutputFormat.setOutputPath(job2, outputDirStep2);

			// Specify the class of the Driver for this job
			job2.setJarByClass(DriverBigData.class);

			// Set job input format
			job2.setInputFormatClass(KeyValueTextInputFormat.class);

			// Set job output format
			job2.setOutputFormatClass(TextOutputFormat.class);

			// Set map class
			job2.setMapperClass(MapperBigDataStep2.class);

			// Set map output key and value classes
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(DoubleWritable.class);

			// Set reduce class
			job2.setReducerClass(ReducerBigDataStep2.class);

			// Set reduce output key and value classes
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);

			// Set number of reducers
			/* NOTE: Probably is a better idea to use a smaller number of reducers, because
			 * the second one is less complex
			 */
			job2.setNumReduceTasks(numberOfReducers);

			// Execute the job and wait for completion
			if (job2.waitForCompletion(true) == true)
				exitCode = 0;
			else
				exitCode = 1;

		}

		return exitCode;
	}

	/**
	 * Main of the driver
	 */

	public static void main(String args[]) throws Exception {
		// Exploit the ToolRunner class to "configure" and run the Hadoop
		// application
		int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

		System.exit(res);
	}
}