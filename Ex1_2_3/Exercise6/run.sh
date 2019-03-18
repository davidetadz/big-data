# Remove folders of the previous run
hdfs dfs -rm -r ex6_data
hdfs dfs -rm -r ex6_out

# Put input data collection into hdfs
hdfs dfs -put ex6_data


# Run application
hadoop jar target/Exercise6-1.0.0.jar it.polito.bigdata.hadoop.exercise6.DriverBigData 1 ex6_data/  ex6_out



