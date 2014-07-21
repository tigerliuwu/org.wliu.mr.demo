package org.talend.reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.talend.map.input.row1Struct;
import org.talend.reduce.output.rowKeyAggStruct;
import org.talend.reduce.output.rowValueAggStruct;

public class MultiOutputMapper extends  Mapper<NullWritable, row1Struct, rowKeyAggStruct, rowValueAggStruct> {

	
	Map<rowKeyAggStruct, rowValueAggStruct> mapOut = new HashMap<rowKeyAggStruct, rowValueAggStruct>();
	MultipleOutputs<rowKeyAggStruct, rowValueAggStruct> outs = null;
	
	protected void setup(Context context) throws IOException, InterruptedException { 
		outs = new MultipleOutputs<rowKeyAggStruct, rowValueAggStruct>(context);
	}
	
	protected void map(NullWritable key, row1Struct value, 
              Context context) throws IOException, InterruptedException {

		rowKeyAggStruct outKey = new rowKeyAggStruct();
		outKey.sex = value.sex;
		rowValueAggStruct outValue = mapOut.get(outKey);
		if (outValue==null) {
			outValue = new rowValueAggStruct();
			mapOut.put(outKey, outValue);
		}
		
		org.talend.map.output.row5Struct row6Value = null;
		
		if (value.age!=null && value.age.intValue()>=20) {
			if (outValue.age_max == null) {
				outValue.age_max = value.age;
			} else if (value.age !=null){
				if (outValue.age_max.intValue() < value.age.intValue()) {
					outValue.age_max = value.age;
				}
			}
			
			if (outValue.age_sum == null) {
				outValue.age_sum = value.age;
			} else if (value.age != null) {
				outValue.age_sum = outValue.age_sum.intValue() + value.age.intValue();
			}
		} else {
			row6Value = new org.talend.map.output.row5Struct();
			row6Value.name = value.name;
			row6Value.ID = value.ID;
			row6Value.sex = value.sex;
			row6Value.errorMessage = "reject row age < 20";
		}
		if (row6Value != null) {
			outs.write("row6", NullWritable.get(), row6Value);
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (Map.Entry<rowKeyAggStruct, rowValueAggStruct> entry : mapOut.entrySet()) {
			context.write(entry.getKey(), entry.getValue());
		}
		if (outs !=null) {
			outs.close();
		}
	}
}
