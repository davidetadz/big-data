# Remove folders of the previous run
hdfs dfs -rm -r ex11_data
hdfs dfs -rm -r ex11_out

# Put input data collection into hdfs
hdfs dfs -put ex11_data


# Run application
hadoop jar target/Exercise11-1.0.0.jar it.polito.bigdata.hadoop.exercise11.DriverBigData 1 ex11_data/  ex11_out



