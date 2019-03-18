# Remove folders of the previous run
hdfs dfs -rm -r ex2_data
hdfs dfs -rm -r ex2_out

# Put input data collection into hdfs
hdfs dfs -put ex2_data


# Run application
hadoop jar target/Exercise2-1.0.0.jar it.polito.bigdata.hadoop.exercise2.DriverBigData 1 ex2_data/  ex2_out




