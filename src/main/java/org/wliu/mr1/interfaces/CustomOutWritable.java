package org.wliu.mr1.interfaces;

import java.io.DataOutput;

import org.apache.hadoop.io.Writable;

public interface CustomOutWritable extends Writable {
	
	void setFieldSeparator(String fieldSep);
	void writeRecord(DataOutput output);

}
