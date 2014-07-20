package org.talend.reduce.output;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.talend.map.output.HDFSRecordWriter;

public class tHDFSOutput_1HDFSRecordWriter extends HDFSRecordWriter<NullWritable, row4Struct> {
	
	public tHDFSOutput_1HDFSRecordWriter(DataOutputStream out) {
		super(out);
	}

	protected void writeObject(row4Struct value) throws IOException {
		System.out.println("======write start=========");
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

		this.out.write(sb.toString().getBytes());
		System.out.println("======write end=========");
	}

}
