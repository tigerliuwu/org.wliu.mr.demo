package org.wliu.mr1.record;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.wliu.mr1.interfaces.CustomOutWritable;

public class SampleRowReduceStruct implements CustomOutWritable {
	
	public String name;
	public int max_age;
//	public String sex;
	private String fieldSeparator=";";

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		Text.writeString(out, name);
		out.write(max_age);
//		Text.writeString(out, sex);
	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.name = Text.readString(in);
		this.max_age =in.readInt();
//		this.sex = Text.readString(in);
	}
	
	public void setFieldSeparator(String sep) {
		this.fieldSeparator = sep;
	}
	
	public void writeRecord(DataOutput output) {
		try {
			output.write(name.getBytes("UTF-8"));
			output.write(this.fieldSeparator.getBytes("UTF-8"));
			output.write(String.valueOf(this.max_age).getBytes("UTF-8"));
//			output.write(this.fieldSeparator.getBytes("UTF-8"));
//			output.write(this.sex.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	

}
