package org.wliu.mr1.example.muloutput;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
/**
 * refer to http://www.cnblogs.com/liangzh/archive/2012/05/22/2512264.html
 * @author wliu
 *
 */
public class MultipleOutputMapper extends
		Mapper<LongWritable,Text,Text,IntWritable> {
		private MultipleOutputs<Text,IntWritable> mos;

	   protected void setup(Context context) throws IOException,InterruptedException {
	     mos = new MultipleOutputs<Text,IntWritable>(context);
	   }
	
	   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
	     String line = value.toString();
	     String[] tokens = line.split("-");
	
	     mos.write("MOSInt",new Text(tokens[0]), new IntWritable(Integer.parseInt(tokens[1])));  //（第一处）
	     mos.write("MOSText", new Text(tokens[0]),tokens[2]);
	     mos.write("MOSText", new Text(tokens[0]),line,tokens[0]+"/");  //（第三处）同时也可写到指定的文件或文件夹中
	     
	     context.write(new Text(tokens[0]), new IntWritable(20));
	   }
	
	   protected void cleanup(Context context) throws IOException,InterruptedException {
		   mos.close();
	   }
}
