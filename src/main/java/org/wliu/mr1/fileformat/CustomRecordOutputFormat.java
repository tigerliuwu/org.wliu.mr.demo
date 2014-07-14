package org.wliu.mr1.fileformat;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.util.ReflectionUtils;
import org.wliu.mr1.writer.CustomRecordWriter;

public class CustomRecordOutputFormat<K, V> extends FileOutputFormat<K, V> {

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
	    Configuration conf = job.getConfiguration();
	    boolean isCompressed = getCompressOutput(job);
	    String keyValueSeparator= conf.get("mapred.textoutputformat.separator",
	                                       "\t");
	    System.out.println("************lineseparator:" + keyValueSeparator);
	    CompressionCodec codec = null;
	    String extension = "";
	    if (isCompressed) {
	      Class<? extends CompressionCodec> codecClass = 
	        getOutputCompressorClass(job, GzipCodec.class);
	      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
	      extension = codec.getDefaultExtension();
	    }
	    Path file = getDefaultWorkFile(job, extension);
	    FileSystem fs = file.getFileSystem(conf);
	    if (!isCompressed) {
	      FSDataOutputStream fileOut = fs.create(file, true);
	      return new CustomRecordWriter<K, V>(fileOut, keyValueSeparator);
	    } else {
	      FSDataOutputStream fileOut = fs.create(file, true);
	      return new CustomRecordWriter<K, V>(new DataOutputStream
	                                        (codec.createOutputStream(fileOut)),
	                                        keyValueSeparator);
	    }
	  }

	  public void checkOutputSpecs(JobContext job
              ) throws FileAlreadyExistsException, IOException{
		// Ensure that the output directory is set and not already there
		Path outDir = getOutputPath(job);
		if (outDir == null) {
			throw new InvalidJobConfException("Output directory not set.");
		}
		
		// get delegation token for outDir's file system
		TokenCache.obtainTokensForNamenodes(job.getCredentials(), 
		                       new Path[] {outDir}, 
		                       job.getConfiguration());
		FileSystem fs = outDir.getFileSystem(job.getConfiguration());
		if (fs.exists(outDir)) {
			fs.delete(outDir, true);
		}
}


}
