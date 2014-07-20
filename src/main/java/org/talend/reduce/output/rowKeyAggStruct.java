package org.talend.reduce.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class rowKeyAggStruct implements
			WritableComparable<rowKeyAggStruct> {

		public String sex;

		public int hashCode() {

			final int prime = 31;
			int result = 1;

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
			final rowKeyAggStruct other = (rowKeyAggStruct) obj;

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

			sb.append("sex=" + sex);

			sb.append("]");

			return sb.toString();
		}

		public void write(DataOutput out) throws IOException {

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

				this.sex = null;
			} else {

				int length_sex = in.readInt();
				byte[] bytes_sex = new byte[length_sex];
				in.readFully(bytes_sex, 0, length_sex);
				this.sex = new String(bytes_sex, 0, length_sex, "UTF-8");

			}

		}

		public int compareTo(rowKeyAggStruct other) {

			int returnValue = -1;

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
