package org.wliu.mr1.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class SampleRowReduceStruct implements Writable {
	
	public String name;
	public int max_age;
	public String sex;

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Text.writeString(out, name);
		out.write(max_age);
		Text.writeString(out, sex);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.name = Text.readString(in);
		this.max_age =in.readInt();
		this.sex = Text.readString(in);
	}

}
