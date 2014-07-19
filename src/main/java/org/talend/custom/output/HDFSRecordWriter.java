package org.talend.custom.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class HDFSRecordWriter extends RecordWriter<NullWritable, row3Struct> {
	protected DataOutputStream out;

	public HDFSRecordWriter(DataOutputStream out) {
		this.out = out;
	}
	
	private void writeObject(row3Struct value) throws IOException {
		StringBuilder sb = new StringBuilder();

		if (value.ID != null) {

			sb.append(value.ID);

		}

		sb.append(";");

		if (value.name != null) {

			sb.append(value.name);

		}

		sb.append(";");

		if (value.age != null) {

			sb.append(value.age);

		}

		this.out.write(sb.toString().getBytes());
	}

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
