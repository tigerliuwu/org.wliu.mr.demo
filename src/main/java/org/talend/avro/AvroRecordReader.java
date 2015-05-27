package org.talend.avro;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.talend.map.input.row1Struct;

public class AvroRecordReader extends RecordReader<NullWritable, row1Struct> {
	private org.apache.avro.file.FileReader<org.apache.avro.generic.GenericRecord> reader;
	private long start;
	private long end;
	org.apache.avro.generic.GenericRecord record = null;
	
	protected AvroRecordReader(TaskAttemptContext context, FileSplit split) throws IOException {
		List<org.apache.avro.Schema.Field> fields = new java.util.ArrayList<org.apache.avro.Schema.Field>();
		List<org.apache.avro.Schema> unionSchema = null;

		unionSchema = new java.util.ArrayList<org.apache.avro.Schema>();
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.INT));
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.NULL));
		fields.add(new org.apache.avro.Schema.Field("ID",
				org.apache.avro.Schema.createUnion(unionSchema), null,
				null));

		unionSchema = new java.util.ArrayList<org.apache.avro.Schema>();
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.STRING));
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.NULL));
		fields.add(new org.apache.avro.Schema.Field("name",
				org.apache.avro.Schema.createUnion(unionSchema), null,
				null));

		unionSchema = new java.util.ArrayList<org.apache.avro.Schema>();
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.INT));
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.NULL));
		fields.add(new org.apache.avro.Schema.Field("age",
				org.apache.avro.Schema.createUnion(unionSchema), null,
				null));

		unionSchema = new java.util.ArrayList<org.apache.avro.Schema>();
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.STRING));
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.NULL));
		fields.add(new org.apache.avro.Schema.Field("sex",
				org.apache.avro.Schema.createUnion(unionSchema), null,
				null));

		org.apache.avro.Schema schema = org.apache.avro.Schema
				.createRecord(fields);

		// create file reader
		org.apache.avro.io.DatumReader<org.apache.avro.generic.GenericRecord> datumReader = new org.apache.avro.generic.GenericDatumReader<org.apache.avro.generic.GenericRecord>(
				schema);
		reader = org.apache.avro.file.DataFileReader
				.openReader(
						new org.apache.avro.mapred.FsInput(split
								.getPath(), context.getConfiguration()), datumReader);

		// sync to start
		reader.sync(split.getStart());
		this.start = reader.tell();
		this.end = split.getStart() + split.getLength();

		record = new org.apache.avro.generic.GenericData.Record(schema);	
	}
	
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	row1Struct value = new row1Struct();
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!reader.hasNext() || reader.pastSync(end)) {
			return false;
		}
		record = reader.next(record);

		Object columnObject = null;

		columnObject = record.get("ID");
		if (columnObject != null) {

			value.ID = (Integer) columnObject;

		} else {
			value.ID = null;

		}

		columnObject = record.get("name");
		if (columnObject != null) {

			value.name = columnObject.toString();

		} else {
			value.name = null;

		}

		columnObject = record.get("age");
		if (columnObject != null) {

			value.age = (Integer) columnObject;

		} else {
			value.age = null;

		}

		columnObject = record.get("sex");
		if (columnObject != null) {

			value.sex = columnObject.toString();

		} else {
			value.sex = null;

		}

		return true;
	}

	public NullWritable getCurrentKey() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	public row1Struct getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}
	
	public long getPos() throws IOException{
		return reader.tell();
	}

	public float getProgress() throws IOException, InterruptedException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (getPos() - start)
					/ (float) (end - start));
		}
	}

	public void close() throws IOException {
		// TODO Auto-generated method stub
		if (reader !=null) {
			reader.close();
		}
	}

}
