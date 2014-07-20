package org.talend.map.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public abstract class HDFSRecordWriter extends RecordWriter<NullWritable, row3Struct> {
	protected DataOutputStream out;

	public HDFSRecordWriter(DataOutputStream out) {
		this.out = out;
	}
	
	protected abstract void writeObject(row3Struct value) throws IOException ;

	public void write(NullWritable key, row3Struct value) throws IOException,
			InterruptedException {

		boolean nullValue = value == null;
		if (nullValue) {
			return;
		} else {
			writeObject(value);
		}
		out.write("\n".getBytes());
	}

	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		if(out!=null) {
			out.close();
		}
	}

}
