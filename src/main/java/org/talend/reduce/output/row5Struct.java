package org.talend.reduce.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class row5Struct implements Writable {

	public Integer age_max;

	public String sex;

	public Integer age_sum;

	public String errorMessage;

	public int hashCode() {

		final int prime = 31;
		int result = 1;

		result = prime * result
				+ ((this.age_max == null) ? 0 : this.age_max.hashCode());

		result = prime * result
				+ ((this.sex == null) ? 0 : this.sex.hashCode());

		result = prime * result
				+ ((this.age_sum == null) ? 0 : this.age_sum.hashCode());

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

		if (this.age_max == null) {
			if (other.age_max != null)
				return false;
		} else if (!this.age_max.equals(other.age_max))
			return false;
		if (this.sex == null) {
			if (other.sex != null)
				return false;
		} else if (!this.sex.equals(other.sex))
			return false;
		if (this.age_sum == null) {
			if (other.age_sum != null)
				return false;
		} else if (!this.age_sum.equals(other.age_sum))
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

		sb.append("age_max=" + String.valueOf(age_max));

		sb.append(",sex=" + sex);

		sb.append(",age_sum=" + String.valueOf(age_sum));

		sb.append(",errorMessage=" + errorMessage);

		sb.append("]");

		return sb.toString();
	}

	public void write(DataOutput out) throws IOException {

		if (this.age_max == null) {

			out.writeByte(-1);

		} else {

			out.writeByte(0);

			out.writeInt(this.age_max);

		}

		if (this.sex == null) {

			out.writeByte(-1);

		} else {

			out.writeByte(0);

			byte[] bytes_sex = this.sex.getBytes("UTF-8");
			out.writeInt(bytes_sex.length);
			out.write(bytes_sex);

		}

		if (this.age_sum == null) {

			out.writeByte(-1);

		} else {

			out.writeByte(0);

			out.writeInt(this.age_sum);

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

			this.age_max = null;
		} else {

			this.age_max = in.readInt();

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

			this.age_sum = null;
		} else {

			this.age_sum = in.readInt();

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

}