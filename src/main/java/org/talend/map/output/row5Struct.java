package org.talend.map.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class row5Struct implements WritableComparable<row5Struct> {

		public Integer ID;

		public String name;

		public String sex;

		public String errorMessage;

		public int hashCode() {

			final int prime = 31;
			int result = 1;

			result = prime * result
					+ ((this.ID == null) ? 0 : this.ID.hashCode());

			result = prime * result
					+ ((this.name == null) ? 0 : this.name.hashCode());

			result = prime * result
					+ ((this.sex == null) ? 0 : this.sex.hashCode());

			result = prime
					* result
					+ ((this.errorMessage == null) ? 0 : this.errorMessage
							.hashCode());

			return result;

		}

		public boolean equals(Object obj) {

			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			final row5Struct other = (row5Struct) obj;

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
			if (this.sex == null) {
				if (other.sex != null)
					return false;
			} else if (!this.sex.equals(other.sex))
				return false;
			if (this.errorMessage == null) {
				if (other.errorMessage != null)
					return false;
			} else if (!this.errorMessage.equals(other.errorMessage))
				return false;
			return true;

		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append("[");

			sb.append("ID=" + String.valueOf(ID));

			sb.append(",name=" + name);

			sb.append(",sex=" + sex);

			sb.append(",errorMessage=" + errorMessage);

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

			if (this.sex == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				byte[] bytes_sex = this.sex.getBytes("UTF-8");
				out.writeInt(bytes_sex.length);
				out.write(bytes_sex);

			}

			if (this.errorMessage == null) {

				out.writeByte(-1);

			} else {

				out.writeByte(0);

				byte[] bytes_errorMessage = this.errorMessage.getBytes("UTF-8");
				out.writeInt(bytes_errorMessage.length);
				out.write(bytes_errorMessage);

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

				this.sex = null;
			} else {

				int length_sex = in.readInt();
				byte[] bytes_sex = new byte[length_sex];
				in.readFully(bytes_sex, 0, length_sex);
				this.sex = new String(bytes_sex, 0, length_sex, "UTF-8");

			}

			if (in.readByte() == -1) {

				this.errorMessage = null;
			} else {

				int length_errorMessage = in.readInt();
				byte[] bytes_errorMessage = new byte[length_errorMessage];
				in.readFully(bytes_errorMessage, 0, length_errorMessage);
				this.errorMessage = new String(bytes_errorMessage, 0,
						length_errorMessage, "UTF-8");

			}

		}

		public int compareTo(row5Struct other) {

			int returnValue = -1;

			returnValue = checkNullsAndCompare(this.ID, other.ID);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.name, other.name);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.sex, other.sex);
			if (returnValue != 0) {
				return returnValue;
			}
			returnValue = checkNullsAndCompare(this.errorMessage,
					other.errorMessage);
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