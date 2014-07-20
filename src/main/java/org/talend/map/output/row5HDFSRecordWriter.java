package org.talend.map.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class row5HDFSRecordWriter extends
			RecordWriter<NullWritable, row5Struct> {
		protected DataOutputStream out;

		public row5HDFSRecordWriter(DataOutputStream out) {
			this.out = out;
		}

		private void writeObject(row5Struct value) throws IOException {
			StringBuilder sb = new StringBuilder();

			if (value.ID != null) {

				sb.append(value.ID);

			}

			sb.append(";");

			if (value.name != null) {

				sb.append(value.name);

			}

			sb.append(";");

			if (value.sex != null) {

				sb.append(value.sex);

			}

			sb.append(";");

			if (value.errorMessage != null) {

				sb.append(value.errorMessage);

			}

			this.out.write(sb.toString().getBytes());
		}

		public synchronized void write(NullWritable key, row5Struct value)
				throws IOException {

			boolean nullValue = value == null;
			if (nullValue) {
				return;
			} else {
				writeObject(value);
			}
			out.write("\n".getBytes());
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException,
				InterruptedException {
			out.close();
			
		}
	}