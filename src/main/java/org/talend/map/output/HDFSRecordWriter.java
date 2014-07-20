package org.talend.map.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public abstract class HDFSRecordWriter<K, V> extends RecordWriter<K, V> {
	protected DataOutputStream out;

	public HDFSRecordWriter(DataOutputStream out) {
		this.out = out;
	}
	
	protected abstract void writeObject(V value) throws IOException ;

	public void write(K key, V value) throws IOException,
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
