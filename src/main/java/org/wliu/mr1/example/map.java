package org.wliu.mr1.example;
        
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.talend.hadoop.mapred.lib.MRJobClient;
import org.talend.hadoop.mapred.lib.TalendJob;

import routines.system.ParserUtils;

/*
 * functionality:
 * 1) support -libjars [maven intall jar] to run the mr application remotely
 * 2) remove the destination file
 * @author:wliu
 */

public class map extends Configured implements Tool{
        

	/**
	 * [tHDFSInput_3 mrbegin ] start
	 */

	public static class row1StructInputFormat
			extends
			org.talend.common.format.TDelimitedFileInputFormat<NullWritable, row1Struct> {

		// init
		// public void setConf(Configuration conf){
		// context = new ContextProperties(conf);
		// setInputPath("/user/wliu/multiple/input/in/in1.txt");
		// setSkipLines(0);
		// }

		protected String getInputPath() {
			return "/user/wliu/multiple/input/in/in1.txt";
		}

		public org.apache.hadoop.mapreduce.RecordReader<NullWritable, row1Struct> getRecordReader(
				org.apache.hadoop.mapreduce.InputSplit split,
				org.apache.hadoop.mapreduce.TaskAttemptContext mrContext)
				throws IOException, InterruptedException {
			setSkipLines(0);
			return new HDFSRecordReader(mrContext, (FileSplit) split,
					"\n".getBytes());
		}

		protected static class HDFSRecordReader
				extends
				org.talend.common.format.TDelimitedFileRecordReader<NullWritable, row1Struct> {


			protected HDFSRecordReader(
					org.apache.hadoop.mapreduce.TaskAttemptContext job,
					FileSplit split, byte[] rowSeparator) throws IOException {
				super(job, split, rowSeparator);
			}

			protected Text dummyValue = new Text();
			row1Struct value = new row1Struct();

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

			public NullWritable getCurrentKey() {
				return NullWritable.get();
			}

			public row1Struct getCurrentValue() {
				return value;
			}

		}

	}

	/**
	 * [tHDFSInput_3 mrbegin ] stop
	 */
	/**
	 * [tHDFSInput_3 mrmain ] start
	 */
	/**
	 * [tHDFSInput_3 mrmain ] stop
	 */
	/**
	 * [tHDFSInput_3 mrend ] start
	 */
	/**
	 * [tHDFSInput_3 mrend ] stop
	 */
	/**
	 * [tHDFSInput_2 mrbegin ] start
	 */

	public static class row2StructInputFormat
			extends
			org.talend.common.format.TDelimitedFileInputFormat<NullWritable, row2Struct> {

		// init
		// public void setConf(Configuration conf){
		// context = new ContextProperties(conf);
		// setInputPath("/user/wliu/multiple/input/in/in2.txt");
		// setSkipLines(0);
		// }

		protected String getInputPath() {
			return "/user/wliu/multiple/input/in/in2.txt";
		}

		public org.apache.hadoop.mapreduce.RecordReader<NullWritable, row2Struct> getRecordReader(
				org.apache.hadoop.mapreduce.InputSplit split,
				org.apache.hadoop.mapreduce.TaskAttemptContext mrContext)
				throws IOException, InterruptedException {
			setSkipLines(0);
			return new HDFSRecordReader(mrContext, (FileSplit) split,
					"\n".getBytes());
		}

		protected static class HDFSRecordReader
				extends
				org.talend.common.format.TDelimitedFileRecordReader<NullWritable, row2Struct> {


			protected HDFSRecordReader(
					org.apache.hadoop.mapreduce.TaskAttemptContext job,
					FileSplit split, byte[] rowSeparator) throws IOException {
				super(job, split, rowSeparator);
			}

			protected Text dummyValue = new Text();
			row2Struct value = new row2Struct();

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

			public NullWritable getCurrentKey() {
				return NullWritable.get();
			}

			public row2Struct getCurrentValue() {
				return value;
			}

		}

	}

	/**
	 * [tHDFSInput_2 mrbegin ] stop
	 */
	/**
	 * [tHDFSInput_2 mrmain ] start
	 */
	/**
	 * [tHDFSInput_2 mrmain ] stop
	 */
	/**
	 * [tHDFSInput_2 mrend ] start
	 */
	/**
	 * [tHDFSInput_2 mrend ] stop
	 */
	/**
	 * [tHDFSOutput_1 mrbegin ] start
	 */

	public static class out1StructOutputFormat extends
			FileOutputFormat<NullWritable, out1Struct> {
		protected static class HDFSRecordWriter extends
				RecordWriter<NullWritable, out1Struct> {
			protected DataOutputStream out;

			public HDFSRecordWriter(DataOutputStream out, TaskAttemptContext job) {
				this.out = out;
			}

			private void writeObject(out1Struct value) throws IOException {
				StringBuilder sb = new StringBuilder();

				if (value.ID != null) {

					sb.append(value.ID);

				}

				sb.append(";");

				if (value.name != null) {

					sb.append(value.name);

				}

				this.out.write(sb.toString().getBytes());
			}

			public synchronized void write(NullWritable key, out1Struct value)
					throws IOException {

				boolean nullValue = value == null;
				if (nullValue) {
					return;
				} else {
					writeObject(value);
				}
				out.write("\n".getBytes());
			}

			public void close(TaskAttemptContext arg0) throws IOException,
					InterruptedException {
				out.close();
			}
		}

		private Path getCustomWorkFile(TaskAttemptContext context, String path,
				String extension) throws IOException {
			FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
			Path basePath = committer.getWorkPath();
			Path outPath = new Path(new Path(path), getUniqueFile(context,
					getOutputName(context), extension));
			return outPath;
		}

		public RecordWriter<NullWritable, out1Struct> getRecordWriter(
				TaskAttemptContext job) throws IOException {

			Path file = FileOutputFormat.getOutputPath(job);
			FileSystem fs = file.getFileSystem(job.getConfiguration());
			Path outPath = getCustomWorkFile(job, "/temp/hello/out/out1", "");
			Path oldPath = new Path("/temp/hello/out/out1");
			if (fs.exists(oldPath)) {
				fs.delete(oldPath, true);
			}
			DataOutputStream fileOut = fs.create(outPath, true);

			return new HDFSRecordWriter(fileOut, job);
		}
	}

	/**
	 * [tHDFSOutput_1 mrbegin ] stop
	 */
	/**
	 * [tHDFSOutput_1 mrmain ] start
	 */
	/**
	 * [tHDFSOutput_1 mrmain ] stop
	 */
	/**
	 * [tHDFSOutput_1 mrend ] start
	 */
	/**
	 * [tHDFSOutput_1 mrend ] stop
	 */
	/**
	 * [tMap_1_TMAP_OUT mrconfig ] start
	 */

	public static class TaggedJoinKey_tMap_1Struct implements
			WritableComparable<TaggedJoinKey_tMap_1Struct> {

		public Integer ID;

		int TaggedJoinKey_tMap_1_tag;

		public int hashCode() {

			final int prime = 31;
			int result = 1;

			result = prime * result
					+ ((this.ID == null) ? 0 : this.ID.hashCode());

			return result;

		}

		public boolean equals(Object obj) {

			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final TaggedJoinKey_tMap_1Struct other = (TaggedJoinKey_tMap_1Struct) obj;

			if (this.ID == null) {
				if (other.ID != null)
					return false;
			} else if (!this.ID.equals(other.ID))
				return false;
			return true;

		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append("[");

			sb.append("ID=" + String.valueOf(ID));

			sb.append("]");

			return sb.toString();
		}

		public void write(DataOutput out) throws IOException {

			if (this.ID == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				out.writeInt(this.ID);

			}

			out.writeInt(TaggedJoinKey_tMap_1_tag);

		}

		public void readFields(DataInput in) throws IOException {

			if (in.readByte() == -1) {

				this.ID = null;
			} else {

				this.ID = in.readInt();

			}

			TaggedJoinKey_tMap_1_tag = in.readInt();

		}

		public int compareTo(TaggedJoinKey_tMap_1Struct other) {

			int returnValue = -1;

			returnValue = checkNullsAndCompare(this.ID, other.ID);
			if (returnValue != 0) {
				return returnValue;
			}
			return returnValue;

		}

		private int checkNullsAndCompare(Object object1, Object object2) {
			int returnValue = 0;
			if (object1 instanceof Comparable && object2 instanceof Comparable) {
				returnValue = ((Comparable) object1).compareTo(object2);
			} else if (object1 != null && object2 != null) {
				returnValue = compareStrings(object1.toString(),
						object2.toString());
			} else if (object1 == null && object2 != null) {
				returnValue = 1;
			} else if (object1 != null && object2 == null) {
				returnValue = -1;
			} else {
				returnValue = 0;
			}

			return returnValue;
		}

		private int compareStrings(String string1, String string2) {
			return string1.compareTo(string2);
		}

		public void setTaggedJoinKeyTag(int tagIndex) {
			this.TaggedJoinKey_tMap_1_tag = tagIndex;
		}

		public int getTaggedJoinKeyTag() {
			return TaggedJoinKey_tMap_1_tag;
		}

		public int compareToWithTagged(TaggedJoinKey_tMap_1Struct other) {
			int cmp = this.compareTo(other);
			if (cmp != 0) {
				return cmp;
			} else {
				Integer thisTag = TaggedJoinKey_tMap_1_tag;
				Integer otherTag = other.getTaggedJoinKeyTag();
				return thisTag.compareTo(otherTag);
			}
		}

	}

	public static class TaggedMapOutput_tMap_1Struct implements Writable {
		int tagIndex;
		Writable mapOutputStruct;

		public Writable getData() {
			return mapOutputStruct;
		}

		public void setTagIndex(int tagIndex) {
			this.tagIndex = tagIndex;
		}

		public int getTagIndex() {
			return tagIndex;
		}

		public void setData(Writable mapOutputStruct) {
			this.mapOutputStruct = mapOutputStruct;
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(tagIndex);
			mapOutputStruct.write(out);
		}

		public void readFields(DataInput in) throws IOException {
			tagIndex = in.readInt();
			if (tagIndex == 1) {
				this.mapOutputStruct = new row1_filter_tMap_1Struct();

			} else if (tagIndex == 0) {
				this.mapOutputStruct = new row3_filter_tMap_1Struct();

			}
			this.mapOutputStruct.readFields(in);
		}

		public TaggedMapOutput_tMap_1Struct clone(Configuration conf) {
			return (TaggedMapOutput_tMap_1Struct) WritableUtils.clone(this,
					conf);
		}
	}

	public static class row1_filter_tMap_1Struct implements
			WritableComparable<row1_filter_tMap_1Struct> {

		public Integer ID;

		public int hashCode() {

			final int prime = 31;
			int result = 1;

			result = prime * result
					+ ((this.ID == null) ? 0 : this.ID.hashCode());

			return result;

		}

		public boolean equals(Object obj) {

			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final row1_filter_tMap_1Struct other = (row1_filter_tMap_1Struct) obj;

			if (this.ID == null) {
				if (other.ID != null)
					return false;
			} else if (!this.ID.equals(other.ID))
				return false;
			return true;

		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append("[");

			sb.append("ID=" + String.valueOf(ID));

			sb.append("]");

			return sb.toString();
		}

		public void write(DataOutput out) throws IOException {

			if (this.ID == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				out.writeInt(this.ID);

			}

		}

		public void readFields(DataInput in) throws IOException {

			if (in.readByte() == -1) {

				this.ID = null;
			} else {

				this.ID = in.readInt();

			}

		}

		public int compareTo(row1_filter_tMap_1Struct other) {

			int returnValue = -1;

			returnValue = checkNullsAndCompare(this.ID, other.ID);
			if (returnValue != 0) {
				return returnValue;
			}
			return returnValue;

		}

		private int checkNullsAndCompare(Object object1, Object object2) {
			int returnValue = 0;
			if (object1 instanceof Comparable && object2 instanceof Comparable) {
				returnValue = ((Comparable) object1).compareTo(object2);
			} else if (object1 != null && object2 != null) {
				returnValue = compareStrings(object1.toString(),
						object2.toString());
			} else if (object1 == null && object2 != null) {
				returnValue = 1;
			} else if (object1 != null && object2 == null) {
				returnValue = -1;
			} else {
				returnValue = 0;
			}

			return returnValue;
		}

		private int compareStrings(String string1, String string2) {
			return string1.compareTo(string2);
		}

	}

	public static class row1_filter_tMap_1Struct_Comparator extends
			WritableComparator {
		int pos1;
		int pos2;
		int comp = 0;

		protected row1_filter_tMap_1Struct_Comparator() {
			super(row1_filter_tMap_1Struct.class, false);
		}

		public int compare(byte b1[], int s1, int l1, byte b2[], int s2, int l2) {
			try {
				pos1 = s1;
				pos2 = s2;

				boolean null1_ID = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_ID = b2[pos2] == -1;
				pos2 += 1;

				if (null1_ID && !null2_ID) {

					return -1;

				} else if (!null1_ID && null2_ID) {

					return 1;

				} else if (null1_ID && null2_ID) {
					// ignore
				} else {

					int v1_ID = readInt(b1, pos1);
					pos1 += 4;
					int v2_ID = readInt(b2, pos2);
					pos2 += 4;

					if (v1_ID > v2_ID) {

						return 1;

					} else if (v1_ID < v2_ID) {

						return -1;

					}

				}

				return comp;

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

	}

	static {
		WritableComparator.define(row1_filter_tMap_1Struct.class,
				new row1_filter_tMap_1Struct_Comparator());
	}

	public static class row3_filter_tMap_1Struct implements
			WritableComparable<row3_filter_tMap_1Struct> {

		public Integer ID;

		public String name;

		public int hashCode() {

			final int prime = 31;
			int result = 1;

			result = prime * result
					+ ((this.ID == null) ? 0 : this.ID.hashCode());

			result = prime * result
					+ ((this.name == null) ? 0 : this.name.hashCode());

			return result;

		}

		public boolean equals(Object obj) {

			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final row3_filter_tMap_1Struct other = (row3_filter_tMap_1Struct) obj;

			if (this.ID == null) {
				if (other.ID != null)
					return false;
			} else if (!this.ID.equals(other.ID))
				return false;
			if (this.name == null) {
				if (other.name != null)
					return false;
			} else if (!this.name.equals(other.name))
				return false;
			return true;

		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append("[");

			sb.append("ID=" + String.valueOf(ID));

			sb.append(",name=" + name);

			sb.append("]");

			return sb.toString();
		}

		public void write(DataOutput out) throws IOException {

			if (this.ID == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				out.writeInt(this.ID);

			}

			if (this.name == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				byte[] bytes_name = this.name.getBytes("UTF-8");
				out.writeInt(bytes_name.length);
				out.write(bytes_name);

			}

		}

		public void readFields(DataInput in) throws IOException {

			if (in.readByte() == -1) {

				this.ID = null;
			} else {

				this.ID = in.readInt();

			}

			if (in.readByte() == -1) {

				this.name = null;
			} else {

				int length_name = in.readInt();
				byte[] bytes_name = new byte[length_name];
				in.readFully(bytes_name, 0, length_name);
				this.name = new String(bytes_name, 0, length_name, "UTF-8");

			}

		}

		public int compareTo(row3_filter_tMap_1Struct other) {

			int returnValue = -1;

			returnValue = checkNullsAndCompare(this.ID, other.ID);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.name, other.name);
			if (returnValue != 0) {
				return returnValue;
			}
			return returnValue;

		}

		private int checkNullsAndCompare(Object object1, Object object2) {
			int returnValue = 0;
			if (object1 instanceof Comparable && object2 instanceof Comparable) {
				returnValue = ((Comparable) object1).compareTo(object2);
			} else if (object1 != null && object2 != null) {
				returnValue = compareStrings(object1.toString(),
						object2.toString());
			} else if (object1 == null && object2 != null) {
				returnValue = 1;
			} else if (object1 != null && object2 == null) {
				returnValue = -1;
			} else {
				returnValue = 0;
			}

			return returnValue;
		}

		private int compareStrings(String string1, String string2) {
			return string1.compareTo(string2);
		}

	}

	public static class row3_filter_tMap_1Struct_Comparator extends
			WritableComparator {
		int pos1;
		int pos2;
		int comp = 0;

		protected row3_filter_tMap_1Struct_Comparator() {
			super(row3_filter_tMap_1Struct.class, false);
		}

		public int compare(byte b1[], int s1, int l1, byte b2[], int s2, int l2) {
			try {
				pos1 = s1;
				pos2 = s2;

				boolean null1_ID = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_ID = b2[pos2] == -1;
				pos2 += 1;

				if (null1_ID && !null2_ID) {

					return -1;

				} else if (!null1_ID && null2_ID) {

					return 1;

				} else if (null1_ID && null2_ID) {
					// ignore
				} else {

					int v1_ID = readInt(b1, pos1);
					pos1 += 4;
					int v2_ID = readInt(b2, pos2);
					pos2 += 4;

					if (v1_ID > v2_ID) {

						return 1;

					} else if (v1_ID < v2_ID) {

						return -1;

					}

				}

				boolean null1_name = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_name = b2[pos2] == -1;
				pos2 += 1;

				if (null1_name && !null2_name) {

					return -1;

				} else if (!null1_name && null2_name) {

					return 1;

				} else if (null1_name && null2_name) {
					// ignore
				} else {

					int len1_name = readInt(b1, pos1);
					pos1 += 4;
					byte[] bs1_name = new byte[len1_name];
					for (int i = 0; i < bs1_name.length; i++) {
						bs1_name[i] = b1[pos1 + i];
					}
					pos1 += bs1_name.length;
					String v1_name = new String(bs1_name, "UTF-8");

					int len2_name = readInt(b2, pos2);
					pos2 += 4;
					byte[] bs2_name = new byte[len2_name];
					for (int i = 0; i < bs2_name.length; i++) {
						bs2_name[i] = b2[pos2 + i];
					}
					pos2 += bs2_name.length;
					String v2_name = new String(bs2_name, "UTF-8");

					int comp_name = v1_name.compareTo(v2_name);
					if (comp_name != 0) {
						if (comp_name > 0) {

							return 1;

						} else {

							return -1;

						}

					}

				}

				return comp;

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

	}

	static {
		WritableComparator.define(row3_filter_tMap_1Struct.class,
				new row3_filter_tMap_1Struct_Comparator());
	}

	static class JoinKeyPartitioner_tMap_1
			extends
			org.apache.hadoop.mapreduce.Partitioner<TaggedJoinKey_tMap_1Struct, Writable> {
		public int getPartition(TaggedJoinKey_tMap_1Struct key, Writable value,
				int numPartitions) {
			return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static class JoinKeyGroupingComparator_tMap_1 extends
			WritableComparator {
		int pos1;
		int pos2;
		int comp = 0;

		protected JoinKeyGroupingComparator_tMap_1() {
			super(TaggedJoinKey_tMap_1Struct.class, false);
		}

		public int compare(byte b1[], int s1, int l1, byte b2[], int s2, int l2) {
			try {
				pos1 = s1;
				pos2 = s2;

				boolean null1_ID = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_ID = b2[pos2] == -1;
				pos2 += 1;

				if (null1_ID && !null2_ID) {

					return -1;

				} else if (!null1_ID && null2_ID) {

					return 1;

				} else if (null1_ID && null2_ID) {
					// ignore
				} else {

					int v1_ID = readInt(b1, pos1);
					pos1 += 4;
					int v2_ID = readInt(b2, pos2);
					pos2 += 4;

					if (v1_ID > v2_ID) {

						return 1;

					} else if (v1_ID < v2_ID) {

						return -1;

					}

				}

				return comp;

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

	}

	public static class JoinKeySortComparator_tMap_1 extends WritableComparator {
		int pos1;
		int pos2;
		int comp = 0;

		protected JoinKeySortComparator_tMap_1() {
			super(TaggedJoinKey_tMap_1Struct.class, false);
		}

		public int compare(byte b1[], int s1, int l1, byte b2[], int s2, int l2) {
			try {
				pos1 = s1;
				pos2 = s2;

				boolean null1_ID = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_ID = b2[pos2] == -1;
				pos2 += 1;

				if (null1_ID && !null2_ID) {

					return -1;

				} else if (!null1_ID && null2_ID) {

					return 1;

				} else if (null1_ID && null2_ID) {
					// ignore
				} else {

					int v1_ID = readInt(b1, pos1);
					pos1 += 4;
					int v2_ID = readInt(b2, pos2);
					pos2 += 4;

					if (v1_ID > v2_ID) {

						return 1;

					} else if (v1_ID < v2_ID) {

						return -1;

					}

				}

				Integer tagIndex1_tMap_1 = readInt(b1, pos1);
				Integer tagIndex2_tMap_1 = readInt(b2, pos2);
				comp = tagIndex1_tMap_1.compareTo(tagIndex2_tMap_1);

				return comp;

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

	}

	/**
	 * [tMap_1_TMAP_OUT mrconfig ] stop
	 */
	public static class tHDFSInput_3Mapper
			extends
			org.talend.samples.base.BasicTalendMapper<NullWritable, row1Struct, WritableComparable, Writable> {
		protected void talendMap(Context mrContext) throws IOException,
				InterruptedException {

			row1Struct row1 = new row1Struct();

			/**
			 * [tMap_1_TMAP_OUT mrbegin ] start
			 */
			TaggedJoinKey_tMap_1Struct key_tMap_1 = new TaggedJoinKey_tMap_1Struct();
			TaggedMapOutput_tMap_1Struct value_tMap_1 = new TaggedMapOutput_tMap_1Struct();
			row1_filter_tMap_1Struct row1Filter_tMap_1 = new row1_filter_tMap_1Struct();
			/**
			 * [tMap_1_TMAP_OUT mrbegin ] stop
			 */

			while (mrContext.nextKeyValue()) {
				System.out.println("good to come here");
				row1 = mrContext.getCurrentValue();

				/**
				 * [tMap_1_TMAP_OUT mrmain ] start
				 */
				row1Filter_tMap_1.ID = row1.ID;

				int tagIndex_tMap_1 = 1;
				key_tMap_1.setTaggedJoinKeyTag(tagIndex_tMap_1);

				key_tMap_1.ID = row1.ID;

				value_tMap_1.setTagIndex(tagIndex_tMap_1);
				value_tMap_1.setData(row1Filter_tMap_1);

				/**
				 * [tMap_1_TMAP_OUT mrmain ] stop
				 */
				mrContext.write(key_tMap_1, value_tMap_1);

			} // while

			/**
			 * [tMap_1_TMAP_OUT mrend ] start
			 */
			System.out.println("tmapOut end");

			/**
			 * [tMap_1_TMAP_OUT mrend ] stop
			 */
		} // talendMap
	}

	public static class tHDFSInput_3Reducer
			extends
			org.talend.samples.base.BasicTalendReducer<TaggedJoinKey_tMap_1Struct, TaggedMapOutput_tMap_1Struct, NullWritable, out1Struct> {
		protected void talendReduce(Context mrContext) throws IOException,
				InterruptedException {

			out1Struct out1 = new out1Struct();

			/**
			 * [tMap_1_TMAP_IN mrbegin ] start
			 */
			List<row3_filter_tMap_1Struct> lookuprow3Values = new java.util.ArrayList<row3_filter_tMap_1Struct>();
			row3_filter_tMap_1Struct row3Default = new row3_filter_tMap_1Struct();
			int num = 0;
			/**
			 * [tMap_1_TMAP_IN mrbegin ] stop
			 */
			while (mrContext.nextKey()) {
				TaggedJoinKey_tMap_1Struct key_tMap_1 = mrContext
						.getCurrentKey();
				Iterator<TaggedMapOutput_tMap_1Struct> values_tMap_1 = mrContext
						.getValues().iterator();
				lookuprow3Values.clear();
				System.out.println("current key:" + key_tMap_1.toString() + "\t tag:" + key_tMap_1.getTaggedJoinKeyTag());
				/**
				 * [tMap_1_TMAP_IN mrmain ] start
				 */
				while (values_tMap_1.hasNext()) {
					TaggedMapOutput_tMap_1Struct value_tMap_1 = values_tMap_1
							.next();

					System.out.println("num=" + num +"\t tagIndex ="
							+ value_tMap_1.getTagIndex() + "\t" + value_tMap_1.getData().toString());
					num ++;
					if (0 == value_tMap_1.getTagIndex()) {

						TaggedMapOutput_tMap_1Struct copyValue_tMap_1 = ((TaggedMapOutput_tMap_1Struct) value_tMap_1)
								.clone(mrContext.getConfiguration());
						lookuprow3Values
								.add((row3_filter_tMap_1Struct) copyValue_tMap_1
										.getData());
						continue;
					}

					if (1 == value_tMap_1.getTagIndex()) {

						row1_filter_tMap_1Struct row1 = (row1_filter_tMap_1Struct) value_tMap_1
								.getData();

						boolean innerRejected_tMap_1 = false;

						boolean forceLooprow3 = false;
						if (innerRejected_tMap_1
								|| lookuprow3Values.size() == 0) {

							innerRejected_tMap_1 = true;

							forceLooprow3 = true;
						}
						row3_filter_tMap_1Struct row3 = null;
						for (int i_row3 = 0; i_row3 < lookuprow3Values.size()
								|| forceLooprow3; i_row3++) {
							if (forceLooprow3) {
								row3 = row3Default;
								i_row3 = lookuprow3Values.size();
								forceLooprow3 = false;
							} else {
								row3 = lookuprow3Values.get(i_row3);

							}

							if (!innerRejected_tMap_1) {

								out1 = null;

								out1 = new out1Struct();

								out1.ID = row1.ID;

								out1.name = row3.name;

							}

							innerRejected_tMap_1 = false;
							/**
							 * [tMap_1_TMAP_IN mrmain ] stop
							 */
//							System.out.println("good to come here");
							// Start of branch "out1"
							if (out1 != null) {
								
								System.out.println("excellent to come here:" + out1.toString());
								mOuts.write("out1", NullWritable.get(), out1);

							} // End of branch "out1"

						} // while
						/**
						 * [tMap_1_TMAP_IN mrend ] start
						 */
					} // for row3

				} // if
			} // while
			/**
			 * [tMap_1_TMAP_IN mrend ] stop
			 */

		} // talendMap
	}

	public static class tHDFSInput_2Mapper
			extends
			org.talend.samples.base.BasicTalendMapper<NullWritable, row2Struct, WritableComparable, Writable> {
		protected void talendMap(Context mrContext) throws IOException,
				InterruptedException {

			row2Struct row2 = new row2Struct();
			row3Struct row3 = new row3Struct();

			/**
			 * [tMap_1_TMAP_OUT mrbegin ] start
			 */
			TaggedJoinKey_tMap_1Struct key_tMap_1 = new TaggedJoinKey_tMap_1Struct();
			TaggedMapOutput_tMap_1Struct value_tMap_1 = new TaggedMapOutput_tMap_1Struct();
			row3_filter_tMap_1Struct row3Filter_tMap_1 = new row3_filter_tMap_1Struct();
			/**
			 * [tMap_1_TMAP_OUT mrbegin ] stop
			 */

			/**
			 * [tFilterRow_4 mrbegin ] start
			 */
			int nb_line_tFilterRow_4 = 0;
			int nb_line_ok_tFilterRow_4 = 0;
			int nb_line_reject_tFilterRow_4 = 0;

			class Operator_tFilterRow_4 {
				private String sErrorMsg = "";
				private boolean bMatchFlag = true;
				private String sUnionFlag = "&&";

				public Operator_tFilterRow_4(String unionFlag) {
					sUnionFlag = unionFlag;
					bMatchFlag = "||".equals(unionFlag) ? false : true;
				}

				public String getErrorMsg() {
					if (sErrorMsg != null && sErrorMsg.length() > 1)
						return sErrorMsg.substring(1);
					else
						return null;
				}

				public boolean getMatchFlag() {
					return bMatchFlag;
				}

				public void matches(boolean partMatched, String reason) {
					// no need to care about the next judgement
					if ("||".equals(sUnionFlag) && bMatchFlag) {
						return;
					}

					if (!partMatched) {
						sErrorMsg += "|" + reason;
					}

					if ("||".equals(sUnionFlag))
						bMatchFlag = bMatchFlag || partMatched;
					else
						bMatchFlag = bMatchFlag && partMatched;
				}
			}
			/**
			 * [tFilterRow_4 mrbegin ] stop
			 */

			while (mrContext.nextKeyValue()) {
				row2 = mrContext.getCurrentValue();

				/**
				 * [tFilterRow_4 mrmain ] start
				 */
				row3 = null;
				Operator_tFilterRow_4 ope_tFilterRow_4 = new Operator_tFilterRow_4(
						"&&");
				ope_tFilterRow_4.matches(
						(row2.age == null ? false
								: row2.age.compareTo(ParserUtils
										.parseTo_Integer(33 + "")) <= 0),
						"age.compareTo(33) <= 0 failed");

				if (ope_tFilterRow_4.getMatchFlag()) {
					if (row3 == null) {
						row3 = new row3Struct();
					}
					row3.ID = row2.ID;
					row3.name = row2.name;
					row3.age = row2.age;
					row3.sex = row2.sex;
					nb_line_ok_tFilterRow_4++;
				} else {
					nb_line_reject_tFilterRow_4++;
				}

				nb_line_tFilterRow_4++;
				/**
				 * [tFilterRow_4 mrmain ] stop
				 */
				// Start of branch "row3"
				if (row3 != null) {

					/**
					 * [tMap_1_TMAP_OUT mrmain ] start
					 */
					row3Filter_tMap_1.ID = row3.ID;

					row3Filter_tMap_1.name = row3.name;

					int tagIndex_tMap_1 = 0;
					key_tMap_1.setTaggedJoinKeyTag(tagIndex_tMap_1);

					key_tMap_1.ID = row3.ID;

					value_tMap_1.setTagIndex(tagIndex_tMap_1);
					value_tMap_1.setData(row3Filter_tMap_1);

					/**
					 * [tMap_1_TMAP_OUT mrmain ] stop
					 */
					mrContext.write(key_tMap_1, value_tMap_1);

				} // End of branch "row3"

			} // while

			/**
			 * [tFilterRow_4 mrend ] start
			 */
			/**
			 * [tFilterRow_4 mrend ] stop
			 */
			/**
			 * [tMap_1_TMAP_OUT mrend ] start
			 */
			System.out.println("tmapOut end");

			/**
			 * [tMap_1_TMAP_OUT mrend ] stop
			 */

		} // talendMap
	}

	public static class row3Struct implements WritableComparable<row3Struct> {

		public Integer ID;

		public String name;

		public Integer age;

		public String sex;

		public int hashCode() {

			final int prime = 31;
			int result = 1;

			result = prime * result
					+ ((this.ID == null) ? 0 : this.ID.hashCode());

			result = prime * result
					+ ((this.name == null) ? 0 : this.name.hashCode());

			result = prime * result
					+ ((this.age == null) ? 0 : this.age.hashCode());

			result = prime * result
					+ ((this.sex == null) ? 0 : this.sex.hashCode());

			return result;

		}

		public boolean equals(Object obj) {

			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final row3Struct other = (row3Struct) obj;

			if (this.ID == null) {
				if (other.ID != null)
					return false;
			} else if (!this.ID.equals(other.ID))
				return false;
			if (this.name == null) {
				if (other.name != null)
					return false;
			} else if (!this.name.equals(other.name))
				return false;
			if (this.age == null) {
				if (other.age != null)
					return false;
			} else if (!this.age.equals(other.age))
				return false;
			if (this.sex == null) {
				if (other.sex != null)
					return false;
			} else if (!this.sex.equals(other.sex))
				return false;
			return true;

		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append("[");

			sb.append("ID=" + String.valueOf(ID));

			sb.append(",name=" + name);

			sb.append(",age=" + String.valueOf(age));

			sb.append(",sex=" + sex);

			sb.append("]");

			return sb.toString();
		}

		public void write(DataOutput out) throws IOException {

			if (this.ID == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				out.writeInt(this.ID);

			}

			if (this.name == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				byte[] bytes_name = this.name.getBytes("UTF-8");
				out.writeInt(bytes_name.length);
				out.write(bytes_name);

			}

			if (this.age == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				out.writeInt(this.age);

			}

			if (this.sex == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				byte[] bytes_sex = this.sex.getBytes("UTF-8");
				out.writeInt(bytes_sex.length);
				out.write(bytes_sex);

			}

		}

		public void readFields(DataInput in) throws IOException {

			if (in.readByte() == -1) {

				this.ID = null;
			} else {

				this.ID = in.readInt();

			}

			if (in.readByte() == -1) {

				this.name = null;
			} else {

				int length_name = in.readInt();
				byte[] bytes_name = new byte[length_name];
				in.readFully(bytes_name, 0, length_name);
				this.name = new String(bytes_name, 0, length_name, "UTF-8");

			}

			if (in.readByte() == -1) {

				this.age = null;
			} else {

				this.age = in.readInt();

			}

			if (in.readByte() == -1) {

				this.sex = null;
			} else {

				int length_sex = in.readInt();
				byte[] bytes_sex = new byte[length_sex];
				in.readFully(bytes_sex, 0, length_sex);
				this.sex = new String(bytes_sex, 0, length_sex, "UTF-8");

			}

		}

		public int compareTo(row3Struct other) {

			int returnValue = -1;

			returnValue = checkNullsAndCompare(this.ID, other.ID);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.name, other.name);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.age, other.age);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.sex, other.sex);
			if (returnValue != 0) {
				return returnValue;
			}
			return returnValue;

		}

		private int checkNullsAndCompare(Object object1, Object object2) {
			int returnValue = 0;
			if (object1 instanceof Comparable && object2 instanceof Comparable) {
				returnValue = ((Comparable) object1).compareTo(object2);
			} else if (object1 != null && object2 != null) {
				returnValue = compareStrings(object1.toString(),
						object2.toString());
			} else if (object1 == null && object2 != null) {
				returnValue = 1;
			} else if (object1 != null && object2 == null) {
				returnValue = -1;
			} else {
				returnValue = 0;
			}

			return returnValue;
		}

		private int compareStrings(String string1, String string2) {
			return string1.compareTo(string2);
		}

	}

	public static class row3Struct_Comparator extends WritableComparator {
		int pos1;
		int pos2;
		int comp = 0;

		protected row3Struct_Comparator() {
			super(row3Struct.class, false);
		}

		public int compare(byte b1[], int s1, int l1, byte b2[], int s2, int l2) {
			try {
				pos1 = s1;
				pos2 = s2;

				boolean null1_ID = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_ID = b2[pos2] == -1;
				pos2 += 1;

				if (null1_ID && !null2_ID) {

					return -1;

				} else if (!null1_ID && null2_ID) {

					return 1;

				} else if (null1_ID && null2_ID) {
					// ignore
				} else {

					int v1_ID = readInt(b1, pos1);
					pos1 += 4;
					int v2_ID = readInt(b2, pos2);
					pos2 += 4;

					if (v1_ID > v2_ID) {

						return 1;

					} else if (v1_ID < v2_ID) {

						return -1;

					}

				}

				boolean null1_name = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_name = b2[pos2] == -1;
				pos2 += 1;

				if (null1_name && !null2_name) {

					return -1;

				} else if (!null1_name && null2_name) {

					return 1;

				} else if (null1_name && null2_name) {
					// ignore
				} else {

					int len1_name = readInt(b1, pos1);
					pos1 += 4;
					byte[] bs1_name = new byte[len1_name];
					for (int i = 0; i < bs1_name.length; i++) {
						bs1_name[i] = b1[pos1 + i];
					}
					pos1 += bs1_name.length;
					String v1_name = new String(bs1_name, "UTF-8");

					int len2_name = readInt(b2, pos2);
					pos2 += 4;
					byte[] bs2_name = new byte[len2_name];
					for (int i = 0; i < bs2_name.length; i++) {
						bs2_name[i] = b2[pos2 + i];
					}
					pos2 += bs2_name.length;
					String v2_name = new String(bs2_name, "UTF-8");

					int comp_name = v1_name.compareTo(v2_name);
					if (comp_name != 0) {
						if (comp_name > 0) {

							return 1;

						} else {

							return -1;

						}

					}

				}

				boolean null1_age = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_age = b2[pos2] == -1;
				pos2 += 1;

				if (null1_age && !null2_age) {

					return -1;

				} else if (!null1_age && null2_age) {

					return 1;

				} else if (null1_age && null2_age) {
					// ignore
				} else {

					int v1_age = readInt(b1, pos1);
					pos1 += 4;
					int v2_age = readInt(b2, pos2);
					pos2 += 4;

					if (v1_age > v2_age) {

						return 1;

					} else if (v1_age < v2_age) {

						return -1;

					}

				}

				boolean null1_sex = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_sex = b2[pos2] == -1;
				pos2 += 1;

				if (null1_sex && !null2_sex) {

					return -1;

				} else if (!null1_sex && null2_sex) {

					return 1;

				} else if (null1_sex && null2_sex) {
					// ignore
				} else {

					int len1_sex = readInt(b1, pos1);
					pos1 += 4;
					byte[] bs1_sex = new byte[len1_sex];
					for (int i = 0; i < bs1_sex.length; i++) {
						bs1_sex[i] = b1[pos1 + i];
					}
					pos1 += bs1_sex.length;
					String v1_sex = new String(bs1_sex, "UTF-8");

					int len2_sex = readInt(b2, pos2);
					pos2 += 4;
					byte[] bs2_sex = new byte[len2_sex];
					for (int i = 0; i < bs2_sex.length; i++) {
						bs2_sex[i] = b2[pos2 + i];
					}
					pos2 += bs2_sex.length;
					String v2_sex = new String(bs2_sex, "UTF-8");

					int comp_sex = v1_sex.compareTo(v2_sex);
					if (comp_sex != 0) {
						if (comp_sex > 0) {

							return 1;

						} else {

							return -1;

						}

					}

				}

				return comp;

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

	}

	static {
		WritableComparator
				.define(row3Struct.class, new row3Struct_Comparator());
	}

	public static class row2Struct implements WritableComparable<row2Struct> {

		public Integer ID;

		public String name;

		public Integer age;

		public String sex;

		public int hashCode() {

			final int prime = 31;
			int result = 1;

			result = prime * result
					+ ((this.ID == null) ? 0 : this.ID.hashCode());

			result = prime * result
					+ ((this.name == null) ? 0 : this.name.hashCode());

			result = prime * result
					+ ((this.age == null) ? 0 : this.age.hashCode());

			result = prime * result
					+ ((this.sex == null) ? 0 : this.sex.hashCode());

			return result;

		}

		public boolean equals(Object obj) {

			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final row2Struct other = (row2Struct) obj;

			if (this.ID == null) {
				if (other.ID != null)
					return false;
			} else if (!this.ID.equals(other.ID))
				return false;
			if (this.name == null) {
				if (other.name != null)
					return false;
			} else if (!this.name.equals(other.name))
				return false;
			if (this.age == null) {
				if (other.age != null)
					return false;
			} else if (!this.age.equals(other.age))
				return false;
			if (this.sex == null) {
				if (other.sex != null)
					return false;
			} else if (!this.sex.equals(other.sex))
				return false;
			return true;

		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append("[");

			sb.append("ID=" + String.valueOf(ID));

			sb.append(",name=" + name);

			sb.append(",age=" + String.valueOf(age));

			sb.append(",sex=" + sex);

			sb.append("]");

			return sb.toString();
		}

		public void write(DataOutput out) throws IOException {

			if (this.ID == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				out.writeInt(this.ID);

			}

			if (this.name == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				byte[] bytes_name = this.name.getBytes("UTF-8");
				out.writeInt(bytes_name.length);
				out.write(bytes_name);

			}

			if (this.age == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				out.writeInt(this.age);

			}

			if (this.sex == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				byte[] bytes_sex = this.sex.getBytes("UTF-8");
				out.writeInt(bytes_sex.length);
				out.write(bytes_sex);

			}

		}

		public void readFields(DataInput in) throws IOException {

			if (in.readByte() == -1) {

				this.ID = null;
			} else {

				this.ID = in.readInt();

			}

			if (in.readByte() == -1) {

				this.name = null;
			} else {

				int length_name = in.readInt();
				byte[] bytes_name = new byte[length_name];
				in.readFully(bytes_name, 0, length_name);
				this.name = new String(bytes_name, 0, length_name, "UTF-8");

			}

			if (in.readByte() == -1) {

				this.age = null;
			} else {

				this.age = in.readInt();

			}

			if (in.readByte() == -1) {

				this.sex = null;
			} else {

				int length_sex = in.readInt();
				byte[] bytes_sex = new byte[length_sex];
				in.readFully(bytes_sex, 0, length_sex);
				this.sex = new String(bytes_sex, 0, length_sex, "UTF-8");

			}

		}

		public int compareTo(row2Struct other) {

			int returnValue = -1;

			returnValue = checkNullsAndCompare(this.ID, other.ID);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.name, other.name);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.age, other.age);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.sex, other.sex);
			if (returnValue != 0) {
				return returnValue;
			}
			return returnValue;

		}

		private int checkNullsAndCompare(Object object1, Object object2) {
			int returnValue = 0;
			if (object1 instanceof Comparable && object2 instanceof Comparable) {
				returnValue = ((Comparable) object1).compareTo(object2);
			} else if (object1 != null && object2 != null) {
				returnValue = compareStrings(object1.toString(),
						object2.toString());
			} else if (object1 == null && object2 != null) {
				returnValue = 1;
			} else if (object1 != null && object2 == null) {
				returnValue = -1;
			} else {
				returnValue = 0;
			}

			return returnValue;
		}

		private int compareStrings(String string1, String string2) {
			return string1.compareTo(string2);
		}

	}

	public static class row2Struct_Comparator extends WritableComparator {
		int pos1;
		int pos2;
		int comp = 0;

		protected row2Struct_Comparator() {
			super(row2Struct.class, false);
		}

		public int compare(byte b1[], int s1, int l1, byte b2[], int s2, int l2) {
			try {
				pos1 = s1;
				pos2 = s2;

				boolean null1_ID = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_ID = b2[pos2] == -1;
				pos2 += 1;

				if (null1_ID && !null2_ID) {

					return -1;

				} else if (!null1_ID && null2_ID) {

					return 1;

				} else if (null1_ID && null2_ID) {
					// ignore
				} else {

					int v1_ID = readInt(b1, pos1);
					pos1 += 4;
					int v2_ID = readInt(b2, pos2);
					pos2 += 4;

					if (v1_ID > v2_ID) {

						return 1;

					} else if (v1_ID < v2_ID) {

						return -1;

					}

				}

				boolean null1_name = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_name = b2[pos2] == -1;
				pos2 += 1;

				if (null1_name && !null2_name) {

					return -1;

				} else if (!null1_name && null2_name) {

					return 1;

				} else if (null1_name && null2_name) {
					// ignore
				} else {

					int len1_name = readInt(b1, pos1);
					pos1 += 4;
					byte[] bs1_name = new byte[len1_name];
					for (int i = 0; i < bs1_name.length; i++) {
						bs1_name[i] = b1[pos1 + i];
					}
					pos1 += bs1_name.length;
					String v1_name = new String(bs1_name, "UTF-8");

					int len2_name = readInt(b2, pos2);
					pos2 += 4;
					byte[] bs2_name = new byte[len2_name];
					for (int i = 0; i < bs2_name.length; i++) {
						bs2_name[i] = b2[pos2 + i];
					}
					pos2 += bs2_name.length;
					String v2_name = new String(bs2_name, "UTF-8");

					int comp_name = v1_name.compareTo(v2_name);
					if (comp_name != 0) {
						if (comp_name > 0) {

							return 1;

						} else {

							return -1;

						}

					}

				}

				boolean null1_age = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_age = b2[pos2] == -1;
				pos2 += 1;

				if (null1_age && !null2_age) {

					return -1;

				} else if (!null1_age && null2_age) {

					return 1;

				} else if (null1_age && null2_age) {
					// ignore
				} else {

					int v1_age = readInt(b1, pos1);
					pos1 += 4;
					int v2_age = readInt(b2, pos2);
					pos2 += 4;

					if (v1_age > v2_age) {

						return 1;

					} else if (v1_age < v2_age) {

						return -1;

					}

				}

				boolean null1_sex = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_sex = b2[pos2] == -1;
				pos2 += 1;

				if (null1_sex && !null2_sex) {

					return -1;

				} else if (!null1_sex && null2_sex) {

					return 1;

				} else if (null1_sex && null2_sex) {
					// ignore
				} else {

					int len1_sex = readInt(b1, pos1);
					pos1 += 4;
					byte[] bs1_sex = new byte[len1_sex];
					for (int i = 0; i < bs1_sex.length; i++) {
						bs1_sex[i] = b1[pos1 + i];
					}
					pos1 += bs1_sex.length;
					String v1_sex = new String(bs1_sex, "UTF-8");

					int len2_sex = readInt(b2, pos2);
					pos2 += 4;
					byte[] bs2_sex = new byte[len2_sex];
					for (int i = 0; i < bs2_sex.length; i++) {
						bs2_sex[i] = b2[pos2 + i];
					}
					pos2 += bs2_sex.length;
					String v2_sex = new String(bs2_sex, "UTF-8");

					int comp_sex = v1_sex.compareTo(v2_sex);
					if (comp_sex != 0) {
						if (comp_sex > 0) {

							return 1;

						} else {

							return -1;

						}

					}

				}

				return comp;

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

	}

	static {
		WritableComparator
				.define(row2Struct.class, new row2Struct_Comparator());
	}

	public static class out1Struct implements WritableComparable<out1Struct> {

		public Integer ID;

		public String name;

		public int hashCode() {

			final int prime = 31;
			int result = 1;

			result = prime * result
					+ ((this.ID == null) ? 0 : this.ID.hashCode());

			result = prime * result
					+ ((this.name == null) ? 0 : this.name.hashCode());

			return result;

		}

		public boolean equals(Object obj) {

			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final out1Struct other = (out1Struct) obj;

			if (this.ID == null) {
				if (other.ID != null)
					return false;
			} else if (!this.ID.equals(other.ID))
				return false;
			if (this.name == null) {
				if (other.name != null)
					return false;
			} else if (!this.name.equals(other.name))
				return false;
			return true;

		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append("[");

			sb.append("ID=" + String.valueOf(ID));

			sb.append(",name=" + name);

			sb.append("]");

			return sb.toString();
		}

		public void write(DataOutput out) throws IOException {

			if (this.ID == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				out.writeInt(this.ID);

			}

			if (this.name == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				byte[] bytes_name = this.name.getBytes("UTF-8");
				out.writeInt(bytes_name.length);
				out.write(bytes_name);

			}

		}

		public void readFields(DataInput in) throws IOException {

			if (in.readByte() == -1) {

				this.ID = null;
			} else {

				this.ID = in.readInt();

			}

			if (in.readByte() == -1) {

				this.name = null;
			} else {

				int length_name = in.readInt();
				byte[] bytes_name = new byte[length_name];
				in.readFully(bytes_name, 0, length_name);
				this.name = new String(bytes_name, 0, length_name, "UTF-8");

			}

		}

		public int compareTo(out1Struct other) {

			int returnValue = -1;

			returnValue = checkNullsAndCompare(this.ID, other.ID);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.name, other.name);
			if (returnValue != 0) {
				return returnValue;
			}
			return returnValue;

		}

		private int checkNullsAndCompare(Object object1, Object object2) {
			int returnValue = 0;
			if (object1 instanceof Comparable && object2 instanceof Comparable) {
				returnValue = ((Comparable) object1).compareTo(object2);
			} else if (object1 != null && object2 != null) {
				returnValue = compareStrings(object1.toString(),
						object2.toString());
			} else if (object1 == null && object2 != null) {
				returnValue = 1;
			} else if (object1 != null && object2 == null) {
				returnValue = -1;
			} else {
				returnValue = 0;
			}

			return returnValue;
		}

		private int compareStrings(String string1, String string2) {
			return string1.compareTo(string2);
		}

	}

	public static class out1Struct_Comparator extends WritableComparator {
		int pos1;
		int pos2;
		int comp = 0;

		protected out1Struct_Comparator() {
			super(out1Struct.class, false);
		}

		public int compare(byte b1[], int s1, int l1, byte b2[], int s2, int l2) {
			try {
				pos1 = s1;
				pos2 = s2;

				boolean null1_ID = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_ID = b2[pos2] == -1;
				pos2 += 1;

				if (null1_ID && !null2_ID) {

					return -1;

				} else if (!null1_ID && null2_ID) {

					return 1;

				} else if (null1_ID && null2_ID) {
					// ignore
				} else {

					int v1_ID = readInt(b1, pos1);
					pos1 += 4;
					int v2_ID = readInt(b2, pos2);
					pos2 += 4;

					if (v1_ID > v2_ID) {

						return 1;

					} else if (v1_ID < v2_ID) {

						return -1;

					}

				}

				boolean null1_name = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_name = b2[pos2] == -1;
				pos2 += 1;

				if (null1_name && !null2_name) {

					return -1;

				} else if (!null1_name && null2_name) {

					return 1;

				} else if (null1_name && null2_name) {
					// ignore
				} else {

					int len1_name = readInt(b1, pos1);
					pos1 += 4;
					byte[] bs1_name = new byte[len1_name];
					for (int i = 0; i < bs1_name.length; i++) {
						bs1_name[i] = b1[pos1 + i];
					}
					pos1 += bs1_name.length;
					String v1_name = new String(bs1_name, "UTF-8");

					int len2_name = readInt(b2, pos2);
					pos2 += 4;
					byte[] bs2_name = new byte[len2_name];
					for (int i = 0; i < bs2_name.length; i++) {
						bs2_name[i] = b2[pos2 + i];
					}
					pos2 += bs2_name.length;
					String v2_name = new String(bs2_name, "UTF-8");

					int comp_name = v1_name.compareTo(v2_name);
					if (comp_name != 0) {
						if (comp_name > 0) {

							return 1;

						} else {

							return -1;

						}

					}

				}

				return comp;

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

	}

	static {
		WritableComparator
				.define(out1Struct.class, new out1Struct_Comparator());
	}

	public static class row1Struct implements WritableComparable<row1Struct> {

		public Integer ID;

		public String name;

		public Integer age;

		public String sex;

		public int hashCode() {

			final int prime = 31;
			int result = 1;

			result = prime * result
					+ ((this.ID == null) ? 0 : this.ID.hashCode());

			result = prime * result
					+ ((this.name == null) ? 0 : this.name.hashCode());

			result = prime * result
					+ ((this.age == null) ? 0 : this.age.hashCode());

			result = prime * result
					+ ((this.sex == null) ? 0 : this.sex.hashCode());

			return result;

		}

		public boolean equals(Object obj) {

			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final row1Struct other = (row1Struct) obj;

			if (this.ID == null) {
				if (other.ID != null)
					return false;
			} else if (!this.ID.equals(other.ID))
				return false;
			if (this.name == null) {
				if (other.name != null)
					return false;
			} else if (!this.name.equals(other.name))
				return false;
			if (this.age == null) {
				if (other.age != null)
					return false;
			} else if (!this.age.equals(other.age))
				return false;
			if (this.sex == null) {
				if (other.sex != null)
					return false;
			} else if (!this.sex.equals(other.sex))
				return false;
			return true;

		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append("[");

			sb.append("ID=" + String.valueOf(ID));

			sb.append(",name=" + name);

			sb.append(",age=" + String.valueOf(age));

			sb.append(",sex=" + sex);

			sb.append("]");

			return sb.toString();
		}

		public void write(DataOutput out) throws IOException {

			if (this.ID == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				out.writeInt(this.ID);

			}

			if (this.name == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				byte[] bytes_name = this.name.getBytes("UTF-8");
				out.writeInt(bytes_name.length);
				out.write(bytes_name);

			}

			if (this.age == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				out.writeInt(this.age);

			}

			if (this.sex == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				byte[] bytes_sex = this.sex.getBytes("UTF-8");
				out.writeInt(bytes_sex.length);
				out.write(bytes_sex);

			}

		}

		public void readFields(DataInput in) throws IOException {

			if (in.readByte() == -1) {

				this.ID = null;
			} else {

				this.ID = in.readInt();

			}

			if (in.readByte() == -1) {

				this.name = null;
			} else {

				int length_name = in.readInt();
				byte[] bytes_name = new byte[length_name];
				in.readFully(bytes_name, 0, length_name);
				this.name = new String(bytes_name, 0, length_name, "UTF-8");

			}

			if (in.readByte() == -1) {

				this.age = null;
			} else {

				this.age = in.readInt();

			}

			if (in.readByte() == -1) {

				this.sex = null;
			} else {

				int length_sex = in.readInt();
				byte[] bytes_sex = new byte[length_sex];
				in.readFully(bytes_sex, 0, length_sex);
				this.sex = new String(bytes_sex, 0, length_sex, "UTF-8");

			}

		}

		public int compareTo(row1Struct other) {

			int returnValue = -1;

			returnValue = checkNullsAndCompare(this.ID, other.ID);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.name, other.name);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.age, other.age);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.sex, other.sex);
			if (returnValue != 0) {
				return returnValue;
			}
			return returnValue;

		}

		private int checkNullsAndCompare(Object object1, Object object2) {
			int returnValue = 0;
			if (object1 instanceof Comparable && object2 instanceof Comparable) {
				returnValue = ((Comparable) object1).compareTo(object2);
			} else if (object1 != null && object2 != null) {
				returnValue = compareStrings(object1.toString(),
						object2.toString());
			} else if (object1 == null && object2 != null) {
				returnValue = 1;
			} else if (object1 != null && object2 == null) {
				returnValue = -1;
			} else {
				returnValue = 0;
			}

			return returnValue;
		}

		private int compareStrings(String string1, String string2) {
			return string1.compareTo(string2);
		}

	}

	public static class row1Struct_Comparator extends WritableComparator {
		int pos1;
		int pos2;
		int comp = 0;

		protected row1Struct_Comparator() {
			super(row1Struct.class, false);
		}

		public int compare(byte b1[], int s1, int l1, byte b2[], int s2, int l2) {
			try {
				pos1 = s1;
				pos2 = s2;

				boolean null1_ID = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_ID = b2[pos2] == -1;
				pos2 += 1;

				if (null1_ID && !null2_ID) {

					return -1;

				} else if (!null1_ID && null2_ID) {

					return 1;

				} else if (null1_ID && null2_ID) {
					// ignore
				} else {

					int v1_ID = readInt(b1, pos1);
					pos1 += 4;
					int v2_ID = readInt(b2, pos2);
					pos2 += 4;

					if (v1_ID > v2_ID) {

						return 1;

					} else if (v1_ID < v2_ID) {

						return -1;

					}

				}

				boolean null1_name = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_name = b2[pos2] == -1;
				pos2 += 1;

				if (null1_name && !null2_name) {

					return -1;

				} else if (!null1_name && null2_name) {

					return 1;

				} else if (null1_name && null2_name) {
					// ignore
				} else {

					int len1_name = readInt(b1, pos1);
					pos1 += 4;
					byte[] bs1_name = new byte[len1_name];
					for (int i = 0; i < bs1_name.length; i++) {
						bs1_name[i] = b1[pos1 + i];
					}
					pos1 += bs1_name.length;
					String v1_name = new String(bs1_name, "UTF-8");

					int len2_name = readInt(b2, pos2);
					pos2 += 4;
					byte[] bs2_name = new byte[len2_name];
					for (int i = 0; i < bs2_name.length; i++) {
						bs2_name[i] = b2[pos2 + i];
					}
					pos2 += bs2_name.length;
					String v2_name = new String(bs2_name, "UTF-8");

					int comp_name = v1_name.compareTo(v2_name);
					if (comp_name != 0) {
						if (comp_name > 0) {

							return 1;

						} else {

							return -1;

						}

					}

				}

				boolean null1_age = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_age = b2[pos2] == -1;
				pos2 += 1;

				if (null1_age && !null2_age) {

					return -1;

				} else if (!null1_age && null2_age) {

					return 1;

				} else if (null1_age && null2_age) {
					// ignore
				} else {

					int v1_age = readInt(b1, pos1);
					pos1 += 4;
					int v2_age = readInt(b2, pos2);
					pos2 += 4;

					if (v1_age > v2_age) {

						return 1;

					} else if (v1_age < v2_age) {

						return -1;

					}

				}

				boolean null1_sex = b1[pos1] == -1;
				pos1 += 1;
				boolean null2_sex = b2[pos2] == -1;
				pos2 += 1;

				if (null1_sex && !null2_sex) {

					return -1;

				} else if (!null1_sex && null2_sex) {

					return 1;

				} else if (null1_sex && null2_sex) {
					// ignore
				} else {

					int len1_sex = readInt(b1, pos1);
					pos1 += 4;
					byte[] bs1_sex = new byte[len1_sex];
					for (int i = 0; i < bs1_sex.length; i++) {
						bs1_sex[i] = b1[pos1 + i];
					}
					pos1 += bs1_sex.length;
					String v1_sex = new String(bs1_sex, "UTF-8");

					int len2_sex = readInt(b2, pos2);
					pos2 += 4;
					byte[] bs2_sex = new byte[len2_sex];
					for (int i = 0; i < bs2_sex.length; i++) {
						bs2_sex[i] = b2[pos2 + i];
					}
					pos2 += bs2_sex.length;
					String v2_sex = new String(bs2_sex, "UTF-8");

					int comp_sex = v1_sex.compareTo(v2_sex);
					if (comp_sex != 0) {
						if (comp_sex > 0) {

							return 1;

						} else {

							return -1;

						}

					}

				}

				return comp;

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

	}

	static {
		WritableComparator
				.define(row1Struct.class, new row1Struct_Comparator());
	}
        
 public static void main(String[] args) throws Exception {
	 int ret = ToolRunner.run(new Configuration(), new map(), args);
	 System.exit(ret);
 }

public int run(String[] args) throws Exception {
	
    Configuration conf = this.getConf();
    
    FileSystem.setDefaultUri(conf, "hdfs://wliu-work:9000");
    conf.set("mapred.job.tracker", "wliu-work:9001");

	final FileSystem fs = FileSystem.get(getConf());
	final Job job = new Job(
			getConf());
	job.setJobName("tHDFSInput_3");
	job.setJarByClass(map.class);

	job.setInputFormatClass(row1StructInputFormat.class);
	job.setMapperClass(tHDFSInput_3Mapper.class);

	job.setReducerClass(tHDFSInput_3Reducer.class);
	

	/**
	 * [tMap_1_TMAP_OUT mrcode ] start
	 */
	job.setMapOutputKeyClass(TaggedJoinKey_tMap_1Struct.class);
	job.setMapOutputValueClass(TaggedMapOutput_tMap_1Struct.class);
	job.setGroupingComparatorClass(JoinKeyGroupingComparator_tMap_1.class);
	job.setSortComparatorClass(JoinKeySortComparator_tMap_1.class);
	job.setPartitionerClass(JoinKeyPartitioner_tMap_1.class);
	
	/**
	 * [tMap_1_TMAP_OUT mrcode ] stop
	 */

//	 row1StructInputFormat.setInputPaths(job,"/user/wliu/multiple/input/in/in1.txt");
	org.apache.hadoop.mapreduce.lib.input.MultipleInputs
	.addInputPath(job, new Path(
			"/tmp", "AAA" + "_" + "map"
					+ "_" + "1.0" + "_"
					+ "tHDFSInput_3_IN1"),
			row1StructInputFormat.class,
			tHDFSInput_3Mapper.class);
	org.apache.hadoop.mapreduce.lib.input.MultipleInputs
			.addInputPath(job, new Path(
					"/tmp", "AAA" + "_" + "map"
							+ "_" + "1.0" + "_"
							+ "tHDFSInput_3_IN2"),
					row2StructInputFormat.class,
					tHDFSInput_2Mapper.class);

	String output = "/temp/hello/out/out1";
	fs.delete(new Path(output), true);
	// job.setOutputFormatClass(out1StructOutputFormat.class);
	// out1StructOutputFormat.setOutputPath(job, new
	// Path(output));
	fs.delete(new Path(
					"/tmp", "AAA" + "_" + "map"
							+ "_" + "1.0" + "_"
							+ "tHDFSInput_3OUT"), true);
	fs.delete(new Path(
			"/tmp", "AAA" + "_" + "map"
					+ "_" + "1.0" + "_"
					+ "tHDFSInput_3"), true);
	org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
			.setOutputPath(job, new Path(new java.io.File(
					"/tmp", "AAA" + "_" + "map"
							+ "_" + "1.0" + "_"
							+ "tHDFSInput_3OUT").toString()));

	org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
			.addNamedOutput(job, "out1",
					out1StructOutputFormat.class,
					NullWritable.class, out1Struct.class);
//	job.submit();
	
	runMRJob(job,1,1);

	return 0;
}

private void runMRJob(Job job, int groupID, int mrjobIDInGroup)
		throws IOException {
	String currentClientPathSeparator = System
			.getProperty("path.separator");
	System.setProperty("path.separator", ":");
	if (job.getConfiguration().get("mapred.reducer.class") == null) {
		job.setNumReduceTasks(0);
	}
	MRJobClient mrJobClient = new MRJobClient(job.getConfiguration());
	mrJobClient.setGroupID(groupID);
	mrJobClient.setMRJobIDInGroup(mrjobIDInGroup);
	mrJobClient.runJob();
	System.setProperty("path.separator", currentClientPathSeparator);
}


        
}