package org.wliu.mr.demo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import org.wliu.mr1.example.WordCountMap;
import org.wliu.mr1.example.WordCountReduce;

public class WordCountTest {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	List<Pair<LongWritable, Text>> inputPairs = new ArrayList<Pair<LongWritable, Text>>(); // 输入数据

	private void prepareInputData() {
		this.inputPairs
				.add(new Pair<LongWritable, Text>(new LongWritable(0), new Text("I guess what you see is what I see")));
	}

	private List<Pair<Text, IntWritable>> getExpectedMapOutput() {
		List<Pair<Text, IntWritable>> outputRecords = new ArrayList<Pair<Text, IntWritable>>();
		// Pair<Text, IntWritable> new Pair<Text, IntWritable>(new Text("I"),
		// new IntWritable(1));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("I"), new IntWritable(1)));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("guess"), new IntWritable(1)));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("what"), new IntWritable(1)));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("you"), new IntWritable(1)));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("see"), new IntWritable(1)));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("is"), new IntWritable(1)));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("what"), new IntWritable(1)));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("I"), new IntWritable(1)));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("see"), new IntWritable(1)));

		return outputRecords;
	}

	private List<Pair<Text, IntWritable>> getExpectedMapReduceOutput() {
		List<Pair<Text, IntWritable>> outputRecords = new ArrayList<Pair<Text, IntWritable>>();
		// Pair<Text, IntWritable> new Pair<Text, IntWritable>(new Text("I"),
		// new IntWritable(1));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("I"), new IntWritable(2)));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("guess"), new IntWritable(1)));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("is"), new IntWritable(1)));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("see"), new IntWritable(2)));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("what"), new IntWritable(2)));
		outputRecords.add(new Pair<Text, IntWritable>(new Text("you"), new IntWritable(1)));

		return outputRecords;
	}

	@Before
	public void setup() {
		WordCountMap mapper = new WordCountMap();
		WordCountReduce reducer = new WordCountReduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

		prepareInputData();
	}

	@Test
	public void testWordCountMap() throws IOException {
		mapDriver.addAll(this.inputPairs);

		mapDriver.withAllOutput(getExpectedMapOutput());
		mapDriver.runTest();
	}

	@Test
	public void testWordCountMapReduce() throws IOException {
		this.mapReduceDriver.addAll(this.inputPairs);
		// this.mapReduceDriver.withMapInputPath(new
		// Path("./mrunit/wordcount.txt"));
		this.mapReduceDriver.addAllOutput(getExpectedMapReduceOutput());
		this.mapReduceDriver.runTest(true);
	}

	@Test
	public void testWordCountJob() throws IOException, ClassNotFoundException, InterruptedException {

		// init the path
		final String input = "testrs/mrunit";
		final String output = "/home/liuwu/tmp/out7";
		FileUtil.fullyDelete(new File(output));
		Configuration conf = new Configuration();
		// conf.addResource("classpath:mapred-site.xml");
		System.err.println(conf.get("mapred.child.java.opts"));
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
