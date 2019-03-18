# Remove folders of the previous run
hdfs dfs -rm -r ex15_data
hdfs dfs -rm -r ex15_out

# Put input data collection into hdfs
hdfs dfs -put ex15_data


# Run application
hadoop jar target/Exercise15-1.0.0.jar it.polito.bigdata.hadoop.exercise15.DriverBigData ex15_data/  ex15_out



