package org.wliu.mr1.example.muloutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * refer to http://www.cnblogs.com/liangzh/archive/2012/05/22/2512264.html
 * @author wliu
 *
 */
public class ReduceMultipleOutputTest extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		String input = "/user/wliu/multiple/in/output";
		String output = "/user/wliu/multiple/out/output";
		Configuration conf = getConf();
		

	    
	    FileSystem.setDefaultUri(conf, "hdfs://wliu-work:9000");
	    conf.set("mapred.job.tracker", "wliu-work:9001");
	     
	    // remove the output directory
	    FileSystem fs = FileSystem.get(conf);
	    fs.delete(new Path(output), true); 
	    
	    Job job = new Job(conf,"word count with ReduceMultipleOutputs");

	    job.setJarByClass(ReduceMultipleOutputTest.class);

	    Path in = new Path(input);
	    Path out = new Path(output);
	    

	    FileInputFormat.setInputPaths(job, in);
	    FileOutputFormat.setOutputPath(job, out);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setMapperClass(ReduceMultipleOutputMapper.class);
	    job.setReducerClass(ReduceMultipleOutputReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setInputFormatClass(TextInputFormat.class);
//	    job.setNumReduceTasks(0);  

	    MultipleOutputs.addNamedOutput(job,"out0",TextOutputFormat.class,Text.class,Text.class);
	    MultipleOutputs.addNamedOutput(job,"out1",TextOutputFormat.class,Text.class,Text.class);
	    MultipleOutputs.addNamedOutput(job,"out2",TextOutputFormat.class,Text.class,IntWritable.class);

	    System.exit(job.waitForCompletion(true)?0:1);
	    return 0;
	  }

	  public static void main(String[] args) throws Exception {

	    int res = ToolRunner.run(new Configuration(), new ReduceMultipleOutputTest(), args);
	    System.exit(res); 
	  }

}
