package org.talend.reduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class tHDFSOutput_2StructOutputFormat extends
		FileOutputFormat<NullWritable, row5Struct> {

			private Path getCustomWorkFile(TaskAttemptContext context, String extension) throws IOException{
				FileOutputCommitter committer = 
					      (FileOutputCommitter) getOutputCommitter(context);
				Path basePath = committer.getWorkPath();
				System.out.println("committer path ="+basePath + "===============");
				Path outPath = new Path(new Path("/user/wliu/multiple/out/out4"), getUniqueFile(context, 
				        getOutputName(context), extension));
				System.out.println("Output path ="+outPath + "===============");
//				Path output2 = FileOutputFormat.getOutputPath(context);
				
				return outPath;
			}
			
	public RecordWriter<NullWritable, row5Struct> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		Path output = FileOutputFormat.getOutputPath(job);
		FileSystem fs = output.getFileSystem(job.getConfiguration());
		
		DataOutputStream out = fs.create(getCustomWorkFile(job,""), true);
		System.out.println("=========open a FSDataOutputStream successfully");
		return new tHDFSOutput_2HDFSRecordWriter(out);
	}
}