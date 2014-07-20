package org.talend.map.input;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.talend.common.format.TDelimitedFileInputFormat;

public class Row1StructInputFormat extends TDelimitedFileInputFormat<NullWritable, row1Struct> {

	@Override
	public RecordReader<NullWritable, row1Struct> getRecordReader(InputSplit split,
			JobContext context) throws IOException, InterruptedException {
		setInputPath("/user/wliu/multiple/input/in/in1.txt");
		setSkipLines(0);		
		return new HDFSRecordReader(context, (FileSplit) split, "\n".getBytes());
	}

}
