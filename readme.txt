1. Maven project
2. hadoop1.0.3 locally
3. java args: -libjars [build jar]
[build jar]:the absolute path of the jar
	for windows, there are 3 things we should care:
		1) should append the file:/// as the path prefix. eg. file:///D:/demo.jar or just /D:/demo.jar
		2) configuration.set(
				"yarn.application.classpath",
				"$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$YARN_HOME/*,$YARN_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*,$HADOOP_COMMON_HOME/share/hadoop/common/*,$HADOOP_COMMON_HOME/share/hadoop/common/lib/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*,$HADOOP_YARN_HOME/share/hadoop/yarn/*,$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*");
		3) maybe still need to do: System.setProperty("path.separator", ":");
