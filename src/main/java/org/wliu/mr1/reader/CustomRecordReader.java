package org.wliu.mr1.reader;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;
import org.wliu.mr1.record.SampleRowStruct;
/**
 * The record reader breaks the data into key/value pairs for input to the
 * {@link Mapper}.
 * @param <KEYIN>
 * @param <VALUEIN>
 */
public class CustomRecordReader extends RecordReader<NullWritable, SampleRowStruct> {
	
	  private static final Log LOG = LogFactory.getLog(LineRecordReader.class);
	  
	  private static String defaultUTF = "UTF-8";

	  private CompressionCodecFactory compressionCodecs = null;
	  private long start;
	  private long pos;
	  private long end;
	  private LineReader in;
	  private int maxLineLength;
	  private SampleRowStruct value = null;
	  private Text cache = null;
	
	
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit)split;
		
		start = fileSplit.getStart();
		end = start + fileSplit.getLength();
		
		this.maxLineLength = context.getConfiguration().getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
		FileSystem fs = fileSplit.getPath().getFileSystem(context.getConfiguration());
		if (fs == null) {
			return;
		}
		FSDataInputStream fsin = fs.open(fileSplit.getPath());
		
		compressionCodecs = new CompressionCodecFactory(context.getConfiguration());
		CompressionCodec codec = compressionCodecs.getCodec(fileSplit.getPath());
		boolean ignorefirstline = false;
		// splitable = false
		if (codec !=null) {
			in = new LineReader((new BufferedInputStream(codec.createInputStream(fsin))));
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				start--;
				ignorefirstline = true;
			}
			in = new LineReader(new BufferedInputStream(fsin));
		}
		if (ignorefirstline = true) {
			start += in.readLine(new Text(), 0,
                    (int)Math.min((long)Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}
	
	
//	public CustomRecordReader() {
//		this(",","\n");
//	}
//	
//	public CustomRecordReader(String fieldSeparator, String lineSeparator) {
//		this.fieldSeparator  = fieldSeparator;
//		this.lineSeparator = lineSeparator;
//		this.bFieldSeparator = this.fieldSeparator.getBytes();
//		this.bLineSeparaotr = this.lineSeparator.getBytes();
//		
//	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (value == null) {
			value = new SampleRowStruct();
		}
		if (cache == null) {
			cache = new Text();
		}
	    int newSize = 0;
	    while (pos < end) {
	      newSize = in.readLine(cache, maxLineLength,
	                            Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
	                                     maxLineLength));
	      if (newSize == 0) {
	        break;
	      }
	      pos += newSize;
	      if (newSize < maxLineLength) {
	        break;
	      }

	      // line too long. try again
	      LOG.info("Skipped line of size " + newSize + " at pos " + 
	               (pos - newSize));
	    }
	    if (newSize == 0) {
	      value = null;
	      return false;
	    } else {
	    	String tempCache = cache.toString();
	    	String[] str = tempCache.split(",", 3);
	    	value.name = str[0];
	    	try {
	    		value.age = Integer.parseInt(str[1]);
	    	} catch (NumberFormatException ex) {
	    		value.age =0;
	    		ex.printStackTrace();
	    	}
	    	value.sex = str[2];
	    	
	    	return true;
	    }
		
	}

	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return NullWritable.get();
	}

	@Override
	public SampleRowStruct getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
	    if (start == end) {
	        return 0.0f;
	      } else {
	        return Math.min(1.0f, (pos - start) / (float)(end - start));
	      }
	}

	@Override
	public void close() throws IOException {
		if(in!=null) {
			in.close();
		}
		
	}

}
