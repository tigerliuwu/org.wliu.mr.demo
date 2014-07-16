package org.wliu.mr1.example.muloutput;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class ReduceMultipleOutputReducer extends
		Reducer<Text, Text, Text, IntWritable> {
	
	private MultipleOutputs<Text, IntWritable> outs = null;
	
	private IntWritable outValue = new IntWritable();

	  /**
	   * Called once at the start of the task.
	   */
	  protected void setup(Context context
	                       ) throws IOException, InterruptedException {
	    outs = new MultipleOutputs<Text, IntWritable>(context);
	  }

	  /**
	   * This method is called once for each key. Most applications will define
	   * their reduce class by overriding this method. The default implementation
	   * is an identity function.
	   */
	  @SuppressWarnings("unchecked")
	  protected void reduce(Text key, Iterable<Text> values, Context context
	                        ) throws IOException, InterruptedException {
		int count = 0;
		Text multiValue = null;
		
	    for(Text value: values) {
	    	count ++ ;
	    	if (count ==1) {
	    		multiValue=value;
	    	}
	    }
	    outValue.set(count);
	    if (count == 0) {
	    	outs.write("out0", key, multiValue);
	    } else if (count ==1) {
	    	outs.write("out1", key, multiValue);
	    } else if (count > 1) {
	    	outs.write("out2", key, outValue);
	    }
	    
	   
	    context.write(key, outValue);
	  }

	  /**
	   * Called once at the end of the task.
	   */
	  protected void cleanup(Context context
	                         ) throws IOException, InterruptedException {
	    outs.close();
	  }
}
