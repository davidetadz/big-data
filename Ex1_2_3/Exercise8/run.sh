# Remove folders of the previous run
hdfs dfs -rm -r ex8_data
hdfs dfs -rm -r ex8_out
hdfs dfs -rm -r ex8_out_final


# Put input data collection into hdfs
hdfs dfs -put ex8_data


# Run application
hadoop jar target/Exercise8-1.0.0.jar it.polito.bigdata.hadoop.exercise8.DriverBigData 2 "ex8_data/*.txt"  ex8_out ex8_out_final



