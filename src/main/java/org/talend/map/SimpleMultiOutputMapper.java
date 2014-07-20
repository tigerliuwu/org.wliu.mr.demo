package org.talend.map;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.talend.custom.input.row1Struct;
import org.talend.custom.output.row3Struct;
import org.talend.custom.output.row5Struct;

public class SimpleMultiOutputMapper extends  Mapper<NullWritable, row1Struct, NullWritable, row3Struct> {

	row3Struct outValue = new row3Struct();
	row5Struct rejectValue = new row5Struct();
	MultipleOutputs<NullWritable, row3Struct> outs = null;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		outs = new MultipleOutputs<NullWritable, row3Struct>(context);
	}
	
	protected void map(NullWritable key, row1Struct value, 
              Context context) throws IOException, InterruptedException {
		 if (value.sex!=null && value.sex.toLowerCase().equals("male")) { // main part
			 outValue.name = value.name;
			 outValue.ID= value.ID;
			 outValue.age = value.age;
			 context.write(NullWritable.get(), outValue);
		 } else {
			 rejectValue.name = value.name;
			 rejectValue.ID = value.ID;
			 rejectValue.sex = value.sex;
			 rejectValue.errorMessage = value.sex +  " is not equals to" + "male";
			 outs.write("row5", key, rejectValue); //, "/user/wliu/multiple/out/out2"
		 }
	  }
	
	   protected void cleanup(Context context) throws IOException,InterruptedException {
		   if(outs !=null) {
			   outs.close();
		   }
	   }
}
