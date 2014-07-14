package org.wliu.mr1.writer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.wliu.mr1.interfaces.CustomOutWritable;

public class CustomRecordWriter<K, V>
    extends RecordWriter<K, V> {
    private static final String utf8 = "UTF-8";
//    private final byte[] newline;


    protected DataOutputStream out;
//    private byte[] fieldSeparator;
    private byte[] lineSeparator;

    public CustomRecordWriter(DataOutputStream out, String lineSeparator) {
      this.out = out;
      try {
//    	  this.fieldSeparator = fieldSeparator.getBytes(utf8);
    	  this.lineSeparator = lineSeparator.getBytes(utf8);
      } catch (UnsupportedEncodingException uee) {
    	  throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
    }

    public CustomRecordWriter(DataOutputStream out) {
      this(out, "\n");
    }

    /**
     * Write the object to the byte stream, handling Text as a special
     * case.
     * @param o the object to print
     * @throws IOException if the write throws, we pass it on
     */
    private void writeObject(V o) throws IOException {
      if (o instanceof CustomOutWritable) {
    	  ((CustomOutWritable)o).writeRecord(out);
      }
    }

    public synchronized void write(K key, V value)
      throws IOException {

      boolean nullValue = value == null;
      if (nullValue) {
        return;
      }

      if (!nullValue) {
        writeObject(value);
      }
      out.write(lineSeparator);
    }

    public synchronized 
    void close(TaskAttemptContext context) throws IOException {
      out.close();
    }



  }