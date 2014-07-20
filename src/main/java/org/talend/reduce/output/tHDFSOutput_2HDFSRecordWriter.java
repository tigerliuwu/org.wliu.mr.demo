package org.talend.reduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.talend.map.output.HDFSRecordWriter;

public class tHDFSOutput_2HDFSRecordWriter extends HDFSRecordWriter<NullWritable, row5Struct> {
	
	public tHDFSOutput_2HDFSRecordWriter(DataOutputStream out) {
		super(out);
	}

	protected void writeObject(row5Struct value) throws IOException {
		StringBuilder sb = new StringBuilder();

		if (value.age_max != null) {

			sb.append(value.age_max);

		}

		sb.append(";");

		if (value.sex != null) {

			sb.append(value.sex);

		}

		sb.append(";");

		if (value.age_sum != null) {

			sb.append(value.age_sum);

		}

		sb.append(";");

		if (value.errorMessage != null) {

			sb.append(value.errorMessage);

		}

		this.out.write(sb.toString().getBytes());
	}

}
