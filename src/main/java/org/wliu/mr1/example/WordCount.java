package org.wliu.mr1.example;
        
import java.io.IOException;
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
	String input = "/user/wliu/wordcount/in/in.txt";
	String output = "/user/wliu/wordcount/out";
	
	// init the mr configuration
    Configuration conf = this.getConf();
    
    FileSystem.setDefaultUri(conf, "hdfs://wliu-work:9000");
    conf.set("mapred.job.tracker", "wliu-work:9001");
    
    // remove the output directory
    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path(output), true);
    
    
    Job job = new Job(conf, "wordcount");

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	    
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	    
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	    
	FileInputFormat.addInputPath(job, new Path(input));
	FileOutputFormat.setOutputPath(job, new Path(output));
	    
	job.waitForCompletion(true);
	return 0;
}
        
}