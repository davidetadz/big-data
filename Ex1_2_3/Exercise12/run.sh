# Remove folders of the previous run
hdfs dfs -rm -r ex12_data
hdfs dfs -rm -r ex12_out

# Put input data collection into hdfs
hdfs dfs -put ex12_data


# Run application
hadoop jar target/Exercise12-1.0.0.jar it.polito.bigdata.hadoop.exercise12.DriverBigData ex12_data/  ex12_out 21



