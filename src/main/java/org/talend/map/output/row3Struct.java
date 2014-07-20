package org.talend.map.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class row3Struct implements WritableComparable<row3Struct> {

		public Integer ID;

		public String name;

		public Integer age;

		public int hashCode() {

			final int prime = 31;
			int result = 1;

			result = prime * result
					+ ((this.ID == null) ? 0 : this.ID.hashCode());

			result = prime * result
					+ ((this.name == null) ? 0 : this.name.hashCode());

			result = prime * result
					+ ((this.age == null) ? 0 : this.age.hashCode());

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
			return true;

		}

		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(super.toString());
			sb.append("[");

			sb.append("ID=" + String.valueOf(ID));

			sb.append(",name=" + name);

			sb.append(",age=" + String.valueOf(age));

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