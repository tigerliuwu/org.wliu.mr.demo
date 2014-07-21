package org.talend.avro;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.talend.map.input.row1Struct;

public class AVROOutputFormat extends FileOutputFormat<NullWritable, row1Struct> {
	void configureDataFileWriter(
			org.apache.avro.file.DataFileWriter<org.apache.avro.generic.GenericRecord> writer,
			Configuration conf) throws java.io.UnsupportedEncodingException {

		writer.setSyncInterval(conf
				.getInt(org.apache.avro.mapred.AvroOutputFormat.SYNC_INTERVAL_KEY,
						org.apache.avro.file.DataFileConstants.DEFAULT_SYNC_INTERVAL));
	}
	
	public final static String EXT = ".avro";
	@Override
	public RecordWriter<NullWritable, row1Struct> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		List<org.apache.avro.Schema.Field> fields = new java.util.ArrayList<org.apache.avro.Schema.Field>();
		List<org.apache.avro.Schema> unionSchema = null;

		unionSchema = new java.util.ArrayList<org.apache.avro.Schema>();
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.INT));
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.NULL));
		fields.add(new org.apache.avro.Schema.Field("ID",
				org.apache.avro.Schema.createUnion(unionSchema), null, null));

		unionSchema = new java.util.ArrayList<org.apache.avro.Schema>();
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.STRING));
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.NULL));
		fields.add(new org.apache.avro.Schema.Field("name",
				org.apache.avro.Schema.createUnion(unionSchema), null, null));

		unionSchema = new java.util.ArrayList<org.apache.avro.Schema>();
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.INT));
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.NULL));
		fields.add(new org.apache.avro.Schema.Field("age",
				org.apache.avro.Schema.createUnion(unionSchema), null, null));

		unionSchema = new java.util.ArrayList<org.apache.avro.Schema>();
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.STRING));
		unionSchema.add(org.apache.avro.Schema
				.create(org.apache.avro.Schema.Type.NULL));
		fields.add(new org.apache.avro.Schema.Field("sex",
				org.apache.avro.Schema.createUnion(unionSchema), null, null));

		org.apache.avro.Schema schema = org.apache.avro.Schema
				.createRecord("record", null, null, false);
		schema.setFields(fields);

		// create file writer
		org.apache.avro.io.DatumWriter<org.apache.avro.generic.GenericRecord> datumWriter = new org.apache.avro.generic.GenericDatumWriter<org.apache.avro.generic.GenericRecord>(
				schema);
		org.apache.avro.file.DataFileWriter<org.apache.avro.generic.GenericRecord> writer = new org.apache.avro.file.DataFileWriter<org.apache.avro.generic.GenericRecord>(
				datumWriter);
		
		Path path = new Path(new Path("/user/wliu/multiple/out/outAVRO"), getUniqueFile(job, 
		        getOutputName(job), EXT));
		configureDataFileWriter(writer, job);
		writer.create(schema, path.getFileSystem(job.getConfiguration()).create(path));

		return new AvroRecordWriter(writer, job, schema);
		
	}
	
	void configureDataFileWriter(
			org.apache.avro.file.DataFileWriter<org.apache.avro.generic.GenericRecord> writer,
			TaskAttemptContext job) throws java.io.UnsupportedEncodingException {

		writer.setSyncInterval(job.getConfiguration()
				.getInt(org.apache.avro.mapred.AvroOutputFormat.SYNC_INTERVAL_KEY,
						org.apache.avro.file.DataFileConstants.DEFAULT_SYNC_INTERVAL));
	}

}
