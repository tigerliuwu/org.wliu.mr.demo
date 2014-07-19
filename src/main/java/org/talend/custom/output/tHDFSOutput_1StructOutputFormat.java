package org.talend.custom.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class tHDFSOutput_1StructOutputFormat extends FileOutputFormat<NullWritable, row3Struct> {

	@Override
	public RecordWriter<NullWritable, row3Struct> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Path output = FileOutputFormat.getOutputPath(job);
		FileSystem fs = output.getFileSystem(job.getConfiguration());
		System.out.println("==========start==============" +output.toString());
		String extension = "";
		Path file = getDefaultWorkFile(job, extension);
//		fs.delete(output, true);
		System.out.println("==========start==============" +file.toString());
		DataOutputStream out = fs.create(file, false);
		System.out.println("==========end==============");
		
		return new HDFSRecordWriter(out);
	}

}
