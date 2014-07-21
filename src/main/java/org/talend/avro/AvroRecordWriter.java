package org.talend.avro;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.talend.map.input.row1Struct;

public class AvroRecordWriter extends RecordWriter<NullWritable, row1Struct> {
	org.apache.avro.file.DataFileWriter<org.apache.avro.generic.GenericRecord> writer = null;
	private org.apache.avro.Schema schema = null;

	public AvroRecordWriter(
			org.apache.avro.file.DataFileWriter<org.apache.avro.generic.GenericRecord> writer,
			TaskAttemptContext context, org.apache.avro.Schema schema) {
		this.writer = writer;
		this.schema = schema;
	}
	
	public void write(NullWritable key, row1Struct value) throws IOException,
			InterruptedException {
		org.apache.avro.generic.GenericRecord record = new org.apache.avro.generic.GenericData.Record(
				schema);

		if (value.ID != null) {

			record.put("ID", value.ID);

		}

		if (value.name != null) {

			record.put("name", value.name);

		}

		if (value.age != null) {

			record.put("age", value.age);

		}

		if (value.sex != null) {

			record.put("sex", value.sex);

		}

		writer.append(record);
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		if (writer != null) {
			writer.close();
		}
	}

}
