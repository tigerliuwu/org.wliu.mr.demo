package org.wliu.mr1.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * functionality:
 * 1) support -libjars [maven intall jar] to run the mr application remotely
 * 2) remove the destination file
 * @author:wliu
 */

public class WordCount {

	public static void main(String[] args) throws Exception {

		// init the path
		final String input = "/home/liuwu/work/gitRepos/org.wliu.mr.demo/src/test/resources/mrunit";
		final String output = "/home/liuwu/tmp/out3";

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count"); // new Job(conf,
														// "wordcount");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		job.setMapperClass(WordCountMap.class);
		job.setReducerClass(WordCountReduce.class);
		job.waitForCompletion(true);

		// this.mapReduceDriver.withMapInputPath(new
		// Path("./mrunit/wordcount.txt"));
		// this.mapReduceDriver.addAllOutput(getExpectedMapReduceOutput());
		// this.mapReduceDriver.runTest();
	}

}