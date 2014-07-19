package org.talend.custom.input;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.talend.common.format.TDelimitedFileRecordReader;

import routines.system.ParserUtils;

public class HDFSRecordReader
				extends
				TDelimitedFileRecordReader<NullWritable, row1Struct> {


			public HDFSRecordReader(JobContext context, FileSplit split,
					byte[] rowSeparator) throws IOException {
				super(context, split, rowSeparator);
			}

			protected Text dummyValue = new Text();
			row1Struct value = new row1Struct();

			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				if (next(dummyValue)) {
					String[] columns = routines.system.StringUtils
							.splitNotRegex(dummyValue.toString(), ";");
					String columnValue = "";

					if (0 < columns.length) {
						columnValue = columns[0];
						if (columnValue.length() > 0) {

							value.ID = ParserUtils.parseTo_Integer(columnValue);

						} else {

							value.ID = null;

						}
					} else {

						value.ID = null;

					}

					if (1 < columns.length) {
						columnValue = columns[1];
						if (columnValue.length() > 0) {

							value.name = columnValue;

						} else {

							value.name = null;

						}
					} else {

						value.name = null;

					}

					if (2 < columns.length) {
						columnValue = columns[2];
						if (columnValue.length() > 0) {

							value.age = ParserUtils
									.parseTo_Integer(columnValue);

						} else {

							value.age = null;

						}
					} else {

						value.age = null;

					}

					if (3 < columns.length) {
						columnValue = columns[3];
						if (columnValue.length() > 0) {

							value.sex = columnValue;

						} else {

							value.sex = null;

						}
					} else {

						value.sex = null;

					}

					return true;
				}
				return false;
			}

			public NullWritable getCurrentKey() throws IOException,
					InterruptedException {
				return NullWritable.get();
			}

			public row1Struct getCurrentValue() throws IOException,
					InterruptedException {
				return value;
			}

		}