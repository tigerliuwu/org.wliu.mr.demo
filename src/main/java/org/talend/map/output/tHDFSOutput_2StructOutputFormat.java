package org.talend.map.output;

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
			private Path getCustomWorkFile(TaskAttemptContext context, String path, String extension) throws IOException{
				FileOutputCommitter committer = 
					      (FileOutputCommitter) getOutputCommitter(context);
				Path basePath = committer.getWorkPath();
				Path outPath = new Path(new Path(path), getUniqueFile(context, 
				        getOutputName(context), extension));
				return outPath;
			}

	@Override
	public RecordWriter<NullWritable, row5Struct> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		Path output = FileOutputFormat.getOutputPath(job);
		FileSystem fs = output.getFileSystem(job.getConfiguration());
		System.out.println("==========start==============" +output.toString());
//		String extension = "";
//		Path file = getDefaultWorkFile(job, extension);
//		fs.delete(output, true);
//		System.out.println("==========start========multipleoutputs======" +file.toString());
//		Path output5 = new Path("/user/wliu/multiple/out/out2");
		
		DataOutputStream out = null;
		Path outPath = getCustomWorkFile(job,"/user/wliu/multiple/out/out2","");
		if(fs.exists(outPath)) {
			fs.delete(outPath, true);
		}
		out = fs.create(outPath, true);
		
		
//		DataOutputStream out = fs.create(getCustomWorkFile(job,""), false);
		System.out.println("==========end==============");

		return new row5HDFSRecordWriter(out);
	}
}