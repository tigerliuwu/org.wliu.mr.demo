package org.talend.common.format;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public abstract class TDelimitedFileRecordReader<K, V> extends
		RecordReader<K, V> {
	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private TDelimitedFileLineReader in;
	int maxLineLength;
	
	public void initialize(InputSplit split,
              TaskAttemptContext context
              ) throws IOException, InterruptedException{
		  
	  }
	
	protected TDelimitedFileRecordReader(TaskAttemptContext context, FileSplit split,
			byte[] rowSeparator) throws IOException {

		this.maxLineLength = Integer.MAX_VALUE;
		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(context.getConfiguration());
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		FileSystem fs = file.getFileSystem(context.getConfiguration());
		FSDataInputStream fileIn = fs.open(file);

		boolean skipFirstLine = false;
		if (codec != null) {
			in = new TDelimitedFileLineReader(codec.createInputStream(fileIn),
					TDelimitedFileLineReader.DEFAULT_BUFFER_SIZE, rowSeparator);
			if (start != 0) {
				skipFirstLine = true;
				start -= rowSeparator.length;
			}
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				skipFirstLine = true;
				start -= rowSeparator.length;
				fileIn.seek(start);
			}
			in = new TDelimitedFileLineReader(fileIn,
					TDelimitedFileLineReader.DEFAULT_BUFFER_SIZE, rowSeparator);
		}
		if (skipFirstLine) {
			start += in.readLine(new Text(), 0,
					(int) Math.min((long) Integer.MAX_VALUE, end - start));
		}
		this.pos = start;
	}

	public boolean next(Text value) throws IOException {
		while (pos < end) {
			int newSize = in.readLine(value, maxLineLength,
					Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
							maxLineLength));
			if (newSize == 0) {
				return false;
			}
			pos += newSize;
			if (newSize < maxLineLength) {
				return true;
			}
		}

		return false;
	}

	public long getPos() throws IOException {
		return pos;
	}

	public void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

	public float getProgress() throws IOException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}
}