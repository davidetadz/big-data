# Remove folders of the previous run
hdfs dfs -rm -r ex4_data
hdfs dfs -rm -r ex4_out

# Put input data collection into hdfs
hdfs dfs -put ex4_data


# Run application
hadoop jar target/Exercise4-1.0.0.jar it.polito.bigdata.hadoop.exercise4.DriverBigData 1 "ex4_data/*.txt"  ex4_out



