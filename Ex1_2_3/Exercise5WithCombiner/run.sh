# Remove folders of the previous run
hdfs dfs -rm -r ex5_data
hdfs dfs -rm -r ex5_out_withcombiner

# Put input data collection into hdfs
hdfs dfs -put ex5_data


# Run application
hadoop jar target/Exercise5WithCombiner-1.0.0.jar it.polito.bigdata.hadoop.exercise5withcombiner.DriverBigData 1 ex5_data/  ex5_out_withcombiner



