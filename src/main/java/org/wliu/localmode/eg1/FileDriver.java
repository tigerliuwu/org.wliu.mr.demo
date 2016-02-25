package org.wliu.localmode.eg1;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FileDriver {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		// init the path
		final String input = "testrs/mrunit";
		final String output = System.getProperty("java.io.tmpdir") + "/tmp/out";
		System.err.println(output);
		FileUtil.fullyDelete(new File(output));

		Job job = Job.getInstance(conf, "new Job");
		job.setJarByClass(FileDriver.class);

		job.setMapperClass(FileMapper.class);
		job.setReducerClass(FileReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));
		job.waitForCompletion(true);
	}
}