# Remove folders of the previous run
hdfs dfs -rm -r ex1_data
hdfs dfs -rm -r ex1_out

# Put input data collection into hdfs
hdfs dfs -put ex1_data


# Run application
hadoop jar target/Exercise1-1.0.0.jar it.polito.bigdata.hadoop.exercise1.DriverBigData 2 ex1_data/  ex1_out




