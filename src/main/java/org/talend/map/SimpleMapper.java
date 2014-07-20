package org.talend.map;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.talend.map.input.row1Struct;
import org.talend.map.output.row3Struct;

public class SimpleMapper extends  Mapper<NullWritable, row1Struct, NullWritable, row3Struct> {

	row3Struct outValue = new row3Struct();
	
	protected void setup(Context context) throws IOException, InterruptedException {
		
	}
	
	protected void map(NullWritable key, row1Struct value, 
              Context context) throws IOException, InterruptedException {
		 if (value.sex!=null && value.sex.toLowerCase().equals("male")) {
			 outValue.name = value.name;
			 outValue.ID= value.ID;
			 outValue.age = value.age;
			 context.write(NullWritable.get(), outValue);
		 }
	  }
}
