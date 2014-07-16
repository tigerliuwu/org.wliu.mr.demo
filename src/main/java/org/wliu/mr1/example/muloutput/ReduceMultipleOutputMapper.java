package org.wliu.mr1.example.muloutput;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * refer to http://www.cnblogs.com/liangzh/archive/2012/05/22/2512264.html
 * @author wliu
 *
 */
public class ReduceMultipleOutputMapper extends
		Mapper<LongWritable,Text,Text,Text> {
	private Text outValue = new Text();

	   protected void setup(Context context) throws IOException,InterruptedException {
		   
	   }
	
	   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	     String line = value.toString();
	     String[] tokens = line.split("-");
	     outValue.set(line);
	     
	     context.write(new Text(tokens[0]), outValue);
	   }
	
	   protected void cleanup(Context context) throws IOException,InterruptedException {
	   }
}
