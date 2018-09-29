hdfs dfs -rm -R /user/rate5786/output/
hdfs dfs -rm -R /user/rate5786/input
hdfs dfs -mkdir /user/rate5786/
hdfs dfs -mkdir /user/rate5786/input/
hdfs dfs -put ~/project-1-hadoop-tellapuram/PartC/TimeBlocks/input/* /user/rate5786/input
hadoop jar ~/project-1-hadoop-tellapuram/PartC/TimeBlocks/Java/TimeBlocks.jar /user/rate5786/input/ /user/rate5786/output/
mkdir ~/project-1-hadoop-tellapuram/PartC/TimeBlocks/output
hdfs dfs -get /user/rate5786/output/* ~/project-1-hadoop-tellapuram/PartC/TimeBlocks/output/
