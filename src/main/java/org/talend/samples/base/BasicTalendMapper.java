package org.talend.samples.base;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public abstract class BasicTalendMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	  protected MultipleOutputs<KEYOUT, VALUEOUT> mOuts = null;
	  protected void setup(Context context
              ) throws IOException, InterruptedException {
		  super.setup(context);
		  mOuts = new MultipleOutputs<KEYOUT, VALUEOUT>(context);
	  }
	  
	  protected void cleanup(Context context
              ) throws IOException, InterruptedException {
		  mOuts.close();
	  }
	  /**
	   * Expert users can override this method for more complete control over the
	   * execution of the Mapper.
	   * @param context
	   * @throws IOException
	   */
	  public void run(Context context) throws IOException, InterruptedException {
	    setup(context);
	    talendMap(context);
	    
	    cleanup(context);
	  }
	  
	  // tIN ---> tFilterRow --row2--> tFilterColumn1 ---> tOut1
	  // tIN ---> tFilterRow --row3--> tFilterColumn2 ---> tOut2
	  abstract protected void talendMap(Context context)  throws IOException, InterruptedException;
		  
		  
		  // tFilterColumn begin start
		  // tfiltercolumn begin end
		  
		  // tfilterRow begin start
		  // tfilterrow begin end
	  /**
		  while (context.nextKeyValue()) {
			VALUEIN value=  context.getCurrentValue();
		   //tfilterrow main start
		   //tfilterrow main end
	  	 if (row2 != null) {
		   //tfiltercolumn1 main start
		  // tfilterColumn1 main end
	  	 mouts.write(out1);
	  	 }
	  		if (row3 != null) {
	  			//tfilterColumn2 main start
	  			//tfilterColumn2 main end
				mouts.write(out2);
	  		}  
		  } 
		  */
// tfilterrow end start
		  // tfilterrow end end
		  // tfiltercolumn end start
		  // tfiltercolumn end end
		  
//	  }
}
