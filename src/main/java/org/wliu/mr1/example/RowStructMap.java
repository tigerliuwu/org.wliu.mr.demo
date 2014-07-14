package org.wliu.mr1.example;
        
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.wliu.mr1.fileformat.CustomFileInputFormat;
import org.wliu.mr1.record.SampleRowStruct;

/*
 * functionality:
 * 1) support -libjars [maven intall jar] to run the mr application remotely
 * 2) remove the destination file
 * @author:wliu
 */

public class RowStructMap extends Configured implements Tool{
        
 public static class Map extends Mapper<NullWritable, SampleRowStruct, Text, SampleRowStruct> {
//    private final static IntWritable one = new IntWritable(1);
//    private SampleRowStruct outValue = new SampleRowStruct();
    private java.util.Map<String, SampleRowStruct> map = new HashMap<String, SampleRowStruct>();
    private Text outKey = new Text();
    
    
    public void map(NullWritable key, SampleRowStruct value, Context context) throws IOException, InterruptedException {
        // calculate the max value for each name
    	if (map.get(value.name)==null) {
        	SampleRowStruct tempValue = new SampleRowStruct();
        	tempValue.name = value.name;
        	tempValue.age = value.age;
        	tempValue.sex = value.sex;
        	map.put(value.name, tempValue);
        } else if (map.get(value.name).age < value.age) {
        	map.get(value.name).age = value.age;
        }
    }
    
    protected void cleanup(Context context
            ) throws IOException, InterruptedException {
    	for (java.util.Map.Entry<String, SampleRowStruct> entry: map.entrySet()) {
    		outKey.set(entry.getKey());
    		context.write(outKey, entry.getValue());
    	}
    }
 } 
        
 public static class Reduce extends Reducer<Text, SampleRowStruct, Text, IntWritable> {
	 private Text outkey = null;
	 private IntWritable outValue = null;
	  protected void setup(Context context
              ) throws IOException, InterruptedException {
		 outkey = new Text();
		 outValue = new IntWritable();
	  }
	  
    public void reduce(Text key, Iterable<SampleRowStruct> values, Context context) 
      throws IOException, InterruptedException {
        int max_age = 0;
        for (SampleRowStruct val : values) {
            if (max_age < val.age) {
            	max_age = val.age;
            }
        }
        
        outValue.set(max_age);
        context.write(key, outValue);
    }
 }
        
 public static void main(String[] args) throws Exception {
	 int ret = ToolRunner.run(new Configuration(), new RowStructMap(), args);
	 System.exit(ret);
 }

public int run(String[] args) throws Exception {
	
	// init the path
	String input = "/user/wliu/in/sample/sample.txt";
	String output = "/user/wliu/wordcount/out";
	
	// init the mr configuration
    Configuration conf = this.getConf();
    
    FileSystem.setDefaultUri(conf, "hdfs://wliu-work:9000");
    conf.set("mapred.job.tracker", "wliu-work:9001");
    
    // remove the output directory
    FileSystem fs = FileSystem.get(conf);
    fs.delete(new Path(output), true);
    
    
    Job job = new Job(conf, "wordcount");
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(SampleRowStruct.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	    
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	    
	job.setInputFormatClass(CustomFileInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	    
	FileInputFormat.addInputPath(job, new Path(input));
	FileOutputFormat.setOutputPath(job, new Path(output));
	    
	job.waitForCompletion(true);
	return 0;
}
        
}