# Remove folders of the previous run
hdfs dfs -rm -r ex9_data
hdfs dfs -rm -r ex9_out

# Put input data collection into hdfs
hdfs dfs -put ex9_data


# Run application
hadoop jar target/Exercise9-1.0.0.jar it.polito.bigdata.hadoop.exercise1.DriverBigData 1 ex9_data/document.txt  ex9_out




