package org.talend.map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.talend.map.input.Row1StructInputFormat;
import org.talend.map.output.row5Struct;
import org.talend.map.output.tHDFSOutput_1StructOutputFormat;
import org.talend.map.output.tHDFSOutput_2StructOutputFormat;

/**
 * refer to http://www.cnblogs.com/liangzh/archive/2012/05/22/2512264.html
 * @author wliu
 *
 */
public class SimpleMapperMultiOutputMain extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		
		String input = "/user/wliu/multiple/input/in/in1.txt";
		String output = "/user/wliu/multiple/out/out1";
		Configuration conf = getConf();
	    
	    FileSystem.setDefaultUri(conf, "hdfs://wliu-work:9000");
	    conf.set("mapred.job.tracker", "wliu-work:9001");
	     
	    // remove the output directory
	    FileSystem fs = FileSystem.get(conf);
	    Path out = new Path(output);

	    Job job = new Job(conf,this.getClass().getCanonicalName());

	    job.setJarByClass(SimpleMapperMultiOutputMain.class);
	    
	    job.setMapperClass(SimpleMultiOutputMapper.class);
	    
	    job.setInputFormatClass(Row1StructInputFormat.class);
	    Row1StructInputFormat.setInputPaths(job, new Path(input));
//	    
	    job.setOutputFormatClass(tHDFSOutput_1StructOutputFormat.class);
	    tHDFSOutput_1StructOutputFormat.setOutputPath(job, out);
	    
	    MultipleOutputs.addNamedOutput(job, "row5", tHDFSOutput_2StructOutputFormat.class, NullWritable.class, row5Struct.class);
	    
	    Path outpath5=new Path("/user/wliu/multiple/out/out2");
//	    tHDFSOutput_2StructOutputFormat.setOutputPath(job, outpath5);
	    
	    
	    fs.delete(out, true);
	    fs.delete(outpath5, true);
	    
	    job.setNumReduceTasks(0);

	    System.exit(job.waitForCompletion(true)?0:1);
	    return 0;
	  }

	  public static void main(String[] args) throws Exception {

	    int res = ToolRunner.run(new Configuration(), new SimpleMapperMultiOutputMain(), args);
	    System.exit(res); 
	  }

}
