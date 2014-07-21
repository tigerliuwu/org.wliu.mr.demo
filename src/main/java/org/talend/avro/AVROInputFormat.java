package org.talend.avro;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.talend.map.input.row1Struct;

public class AVROInputFormat extends FileInputFormat<NullWritable, row1Struct> {
	public final static String EXT = ".avro";

	  protected List<FileStatus> listStatus(JobContext job
              ) throws IOException {
			List<org.apache.hadoop.fs.FileStatus> result = new java.util.ArrayList<org.apache.hadoop.fs.FileStatus>();
			for (org.apache.hadoop.fs.FileStatus file : super.listStatus(job)) {
				if (file.getPath().getName().endsWith(EXT)) {
					result.add(file);
				}
			}
			return result;
	  }
		public List<InputSplit> getSplits(JobContext job)
				throws IOException {

//			context = new ContextProperties(job);
			job.getConfiguration().set("mapred.input.dir", "/tmp"
					+ "/simpleMultiOutputMixReduce/tMROutput_tFilterRow_1/row2");

			return super.getSplits(job);
		}
	public RecordReader<NullWritable, row1Struct> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new AvroRecordReader(context, (FileSplit) split);
	}


}
