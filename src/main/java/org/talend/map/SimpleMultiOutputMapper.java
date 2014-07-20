package org.talend.map;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.talend.map.input.row1Struct;
import org.talend.map.output.row3Struct;
import org.talend.map.output.row5Struct;

public class SimpleMultiOutputMapper extends  Mapper<NullWritable, row1Struct, NullWritable, row3Struct> {

	
	
	MultipleOutputs<NullWritable, row3Struct> outs = null;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		outs = new MultipleOutputs<NullWritable, row3Struct>(context);
	}
	
	protected void map(NullWritable key, row1Struct value, 
              Context context) throws IOException, InterruptedException {
		row3Struct outValue = null; 
		row5Struct rejectValue = null;
		if (value.sex!=null && value.sex.toLowerCase().equals("male")) { // main part
			 outValue = new row3Struct();
			 outValue.name = value.name;
			 outValue.ID= value.ID;
			 outValue.age = value.age;
//			 context.write(NullWritable.get(), outValue);
		 } else {
			 rejectValue = new row5Struct();
			 rejectValue.name = value.name;
			 rejectValue.ID = value.ID;
			 rejectValue.sex = value.sex;
//			 rejectValue.errorMessage = value.sex +  " is not equals to" + "male";
			 
		 }
		if (outValue != null) {
			context.write(NullWritable.get(), outValue);
		}
		if (rejectValue != null) {
			outs.write("row5", key, rejectValue); //, "/user/wliu/multiple/out/out2"
		}
	
	}
	
	   protected void cleanup(Context context) throws IOException,InterruptedException {
		   if(outs !=null) {
			   outs.close();
		   }
	   }
}
