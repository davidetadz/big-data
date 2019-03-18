# Remove folders of the previous run
hdfs dfs -rm -r ex14_data
hdfs dfs -rm -r ex14_out

# Put input data collection into hdfs
hdfs dfs -put ex14_data


# Run application
hadoop jar target/Exercise14-1.0.0.jar it.polito.bigdata.hadoop.exercise14.DriverBigData 1 ex14_data/  ex14_out



