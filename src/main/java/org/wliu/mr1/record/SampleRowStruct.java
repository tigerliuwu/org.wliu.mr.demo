package org.wliu.mr1.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class SampleRowStruct implements Writable {
	
	public String name;
	public int age;
	public String sex;

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Text.writeString(out, name);
		
		Text.writeString(out, sex);
		out.writeInt(age);
		
//		byte[] nameArray = this.name.getBytes("UTF-8");
//		out.writeByte(nameArray.length);
//		out.write(nameArray);
//		
		
		
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.name = Text.readString(in);
		System.out.println("######write name:" + name);
		
		this.sex = Text.readString(in);
		System.out.println("######write sex:" + sex);
		this.age =in.readInt();
		
//		int len_name = in.readInt();
		
	}
	
	public void clear() {
		this.name = null;
		this.age = -1;
		this.sex = null;
	}

}
