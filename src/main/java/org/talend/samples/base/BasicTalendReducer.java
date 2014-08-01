package org.talend.samples.base;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public abstract class BasicTalendReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends
		Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	  public void run(Context context) throws IOException, InterruptedException {
		    setup(context);
		    while (context.nextKey()) {
		      reduce(context.getCurrentKey(), context.getValues(), context);
		    }
		    talendReduce(context);
		    cleanup(context);
	}
	  
	  /**
	   * 
	   * tIN---> tAggregateRow ---> tfilterColumn ---> tOut
	   * @param context
	   * @throws IOException
	   * @throws InterruptedException
	   */
	  protected void talendReduce(Context context)  throws IOException, InterruptedException{
		  // tfilterColumn begin
		  // tAggregateRow begin
		  /**
		  while (context.nextKey()) {
			  KEYIN key = context.getCurrentKey();
			  Iterable<VALUEIN> values = context.getValues();
			  // tAggregateRow main
			  // tFiterColumn main
		  }
		  */
		  // tAggregateRow end
		  // tfilterColumn end
	  }
	
}
