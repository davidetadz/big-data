# Remove folders of the previous run
hdfs dfs -rm -r ex3_data
hdfs dfs -rm -r ex3_out

# Put input data collection into hdfs
hdfs dfs -put ex3_data


# Run application
hadoop jar target/Exercise3-1.0.0.jar it.polito.bigdata.hadoop.exercise3.DriverBigData 1 "ex3_data/*.txt"  ex3_out



