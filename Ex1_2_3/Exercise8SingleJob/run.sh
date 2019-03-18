# Remove folders of the previous run
hdfs dfs -rm -r ex8_data
hdfs dfs -rm -r ex8_outSingle


# Put input data collection into hdfs
hdfs dfs -put ex8_data


# Run application
hadoop jar target/Exercise8SingleJob-1.0.0.jar it.polito.bigdata.hadoop.exercise8.DriverBigData 2 "ex8_data/*.txt"  ex8_outSingle



