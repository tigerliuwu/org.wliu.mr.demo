package org.talend.reduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.talend.reduce.output.row4Struct;
import org.talend.reduce.output.row5Struct;
import org.talend.reduce.output.rowKeyAggStruct;
import org.talend.reduce.output.rowValueAggStruct;

public class CopyOfSimpleMultiOutputReducer extends Reducer<rowKeyAggStruct, rowValueAggStruct, NullWritable, row4Struct> {
	
	MultipleOutputs<NullWritable, row4Struct> outs = null;
	
	protected void setup(Context context) {
		outs = new MultipleOutputs<NullWritable, row4Struct>(context);
	}
	
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
		
		row4Struct out4 = null;
		row5Struct rejectOut4 = null;
		if (outValue.age_max >=30) {
			out4 = new row4Struct();
			out4 = outValue;
			
		} else {
			rejectOut4 = new row5Struct();
			rejectOut4.sex = outValue.sex;
			rejectOut4.age_max = outValue.age_max;
			rejectOut4.age_sum = outValue.age_sum;
		}
		
		if (out4 !=null) {
//			context.write(NullWritable.get(), outValue);
		}
		if (rejectOut4 != null) {
			outs.write("row5", NullWritable.get(), rejectOut4);
		}
		
	}
	
	protected void clearup(Context context) throws IOException, InterruptedException {
		if (outs !=null ) {
			outs.close();
		}
	}
}
