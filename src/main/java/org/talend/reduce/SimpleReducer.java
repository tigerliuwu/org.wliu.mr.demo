package org.talend.reduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.talend.reduce.output.row4Struct;
import org.talend.reduce.output.rowKeyAggStruct;
import org.talend.reduce.output.rowValueAggStruct;

public class SimpleReducer extends Reducer<rowKeyAggStruct, rowValueAggStruct, NullWritable, row4Struct> {
	
	protected void reduce(rowKeyAggStruct key, Iterable<rowValueAggStruct> values, Context context) throws IOException, InterruptedException {
		row4Struct outValue = new row4Struct();
		outValue.sex = key.sex;
		
		for(rowValueAggStruct value: values) {
			if (value.age_max !=null) {
				if (outValue.age_max == null) {
					outValue.age_max = value.age_max;
				} else if (outValue.age_max.intValue() < value.age_max.intValue()){
					outValue.age_max = value.age_max;
				}
			}
			if (value.age_sum != null) {
				if (outValue.age_sum == null) {
					outValue.age_sum = value.age_sum;
				} else {
					outValue.age_sum = outValue.age_sum.intValue() + value.age_sum.intValue();
				}
			}
		}
		
		
		if (outValue.age_max >=30) {
		
			context.write(NullWritable.get(), outValue);
		}
	}
}
