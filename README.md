#mapreduce代码demo

##运行模式
两种运行模式，如下所示：
###local模式
包org.wliu.localmode下的代码不需要创建集群，为本地运行模式

###remote模式
将代码发送到集群的各个计算任务节点运行对应的mapper和reducer代码。<br>
step 1： 将该项目package成一个jar<br>
step 2： 使用ToolRunner.run()调用继承(extends)Configured和实现了Tool的java类<br>
step 2： 使用-libjars package_name.jar 作为program arguments运行<br>

##测试
使用mrunit进行测试，所有相关的测试代码都放在src/test/java文件夹下。

###需要的jar
maven dependency:

  	<dependency>
  		<groupId>org.apache.mrunit</groupId>
  		<artifactId>mrunit</artifactId>
  		<version>1.1.0</version>
  		<classifier>hadoop2</classifier>
	</dependency>
在maven repository搜索mrunit会找到两个hadoop1和hadoop2两种支持，使用classifier进行区分。

###使用Junit4测试框架
导入org.junit里面的类进行junit测试

###要点
1. 对项目的源代码package jar之后，才能运行junit里面的测试单元。（原因未知）<br>
2. testrs/mrunit文件夹下为测试的源文件<br>
