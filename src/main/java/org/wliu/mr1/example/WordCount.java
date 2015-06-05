package org.wliu.mr1.example;
        
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * functionality:
 * 1) support -libjars [maven intall jar] to run the mr application remotely
 * 2) remove the destination file
 * @author:wliu
 */

public class WordCount extends Configured implements Tool{
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
 }
        
 public static void main(String[] args) throws Exception {
	 int ret = ToolRunner.run(new Configuration(), new WordCount(), args);
	 System.exit(ret);
 }

public int run(String[] args) throws Exception {
	
	// init the path
	final String input = "/user/wliu/input/in";
	final String output = "/user/wliu/wordcount/out1";
	
	// init the mr configuration
    final Configuration conf = this.getConf();
    
    FileSystem.setDefaultUri(conf, "hdfs://yarn:9000");
    conf.set("mapreduce.framework.name", "yarn");
    conf.set("yarn.resourcemanager.address", "yarn:8032");
    conf.set("yarn.resourcemanager.scheduler.address", "yarn:8030");
    conf.set("mapreduce.app-submission.cross-platform", "true");
    
    UserGroupInformation ugi = UserGroupInformation
			.createRemoteUser("wliu");
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
		public Void run() throws Exception {
    // remove the output directory
    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path(output), true);
    
    
    Job job = Job.getInstance(conf, "word count"); //new Job(conf, "wordcount");

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	    
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	    
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	    
	FileInputFormat.addInputPath(job, new Path(input));
	FileOutputFormat.setOutputPath(job, new Path(output));
	    
	job.waitForCompletion(true);
	return null;
		}
		});
	return 0;
}
        
}