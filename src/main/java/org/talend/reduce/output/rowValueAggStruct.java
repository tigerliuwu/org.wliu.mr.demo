package org.talend.reduce.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class rowValueAggStruct implements Writable {

	public Integer age_max;

	public Integer age_sum;

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(super.toString());
		sb.append("[");

		sb.append("age_max=" + String.valueOf(age_max));

		sb.append(",age_sum=" + String.valueOf(age_sum));

		sb.append("]");

		return sb.toString();
	}

	public void write(DataOutput out) throws IOException {

		if (age_max == null) {

			out.writeByte(-1);

		} else {

			out.writeByte(0);

			out.writeInt(age_max);

		}

		if (age_sum == null) {

			out.writeByte(-1);

		} else {

			out.writeByte(0);

			out.writeInt(age_sum);

		}

	}

	public void readFields(DataInput in) throws IOException {

		if (in.readByte() == -1) {

			age_max = null;
		} else {

			age_max = in.readInt();

		}

		if (in.readByte() == -1) {

			age_sum = null;
		} else {

			age_sum = in.readInt();

		}

	}

	public void reset() {

		age_sum = null;

	}

}