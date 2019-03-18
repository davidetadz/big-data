# Remove folders of the previous run
hdfs dfs -rm -r ex13_data
hdfs dfs -rm -r ex13_out

# Put input data collection into hdfs
hdfs dfs -put ex13_data


# Run application
hadoop jar target/Exercise13-1.0.0.jar it.polito.bigdata.hadoop.exercise13.DriverBigData ex13_data/  ex13_out



