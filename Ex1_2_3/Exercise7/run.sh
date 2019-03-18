# Remove folders of the previous run
hdfs dfs -rm -r ex7_data
hdfs dfs -rm -r ex7_out

# Put input data collection into hdfs
hdfs dfs -put ex7_data


# Run application
hadoop jar target/Exercise7-1.0.0.jar it.polito.bigdata.hadoop.exercise7.DriverBigData 1 ex7_data/  ex7_out



