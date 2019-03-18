# Remove folders of the previous run
hdfs dfs -rm -r ex10_data
hdfs dfs -rm -r ex10_out

# Put input data collection into hdfs
hdfs dfs -put ex10_data


# Run application
hadoop jar target/Exercise10-1.0.0.jar it.polito.bigdata.hadoop.exercise10.DriverBigData ex10_data/ ex10_out



