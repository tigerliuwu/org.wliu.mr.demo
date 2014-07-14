package org.wliu.mr1.fileformat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.wliu.mr1.reader.CustomRecordReader;
import org.wliu.mr1.record.SampleRowStruct;


public class CustomFileInputFormat extends FileInputFormat<NullWritable, SampleRowStruct> {

	@Override
	public RecordReader<NullWritable, SampleRowStruct> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new CustomRecordReader();
	}
	
	protected boolean isSplitable(JobContext context, Path filename) {
		CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(context.getConfiguration());
		if(compressionCodecs.getCodec(filename) != null) {
			return false;
		}
		return true;
	}

}
