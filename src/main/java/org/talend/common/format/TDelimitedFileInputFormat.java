package org.talend.common.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public abstract class TDelimitedFileInputFormat<K, V> extends
		FileInputFormat<K, V>  {
	  private static final Log LOG = LogFactory.getLog(FileInputFormat.class);

	  private static final double SPLIT_SLOP = 1.1;   // 10% slop
	  
	  public final RecordReader<K,V> createRecordReader(InputSplit split,
	                                         TaskAttemptContext context
	                                        ) throws IOException, 
	                                                 InterruptedException{
		  return getRecordReader(split, context);
	  }
	  public abstract 
	    RecordReader<K,V> getRecordReader(InputSplit split,
	                                         JobContext context
	                                        ) throws IOException, 
	                                                 InterruptedException;

	private String inputPath;
	private int skipLines = 0;

	protected void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}

	protected void setSkipLines(int skipLines) {
		this.skipLines = skipLines;
	}

	  protected List<FileStatus> listStatus(JobContext job
              ) throws IOException {
		  List<FileStatus> status = super.listStatus(job);
		java.util.List<org.apache.hadoop.fs.FileStatus> result = new java.util.ArrayList<org.apache.hadoop.fs.FileStatus>();

		for (int i = 0; status != null && i < status.size(); i++) {
			org.apache.hadoop.fs.FileStatus statu = status.get(i);

			if (statu.isDir()) {
				continue;
			}

			result.add(statu);
		}

		return result;
	}

	  public List<InputSplit> getSplits(JobContext job
              ) throws IOException {
//		job.getConfiguration().set("mapred.input.dir", inputPath);
		List<FileStatus> files = listStatus(job);

		// Save the number of input files in the job-conf
//		job.getConfiguration().setLong("mapreduce.input.num.files", files.size());
//		long totalSize = 0; // compute total size
		for (org.apache.hadoop.fs.FileStatus file : files) { // check we have
																// valid files
			if (file.isDir()) {
				throw new IOException("Not a file: " + file.getPath());
			}
//			totalSize += file.getLen();
		}

	    long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
	    long maxSize = getMaxSplitSize(job);

	    // generate splits
	    List<InputSplit> splits = new ArrayList<InputSplit>();
//	    List<FileStatus> files = listStatus(job);
	    for (FileStatus file: files) {
	      Path path = file.getPath();
	      FileSystem fs = path.getFileSystem(job.getConfiguration());
	      long length = file.getLen();
	      BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0, length);
	      if ((length != 0) && isSplitable(job, path)) { 
	        long blockSize = file.getBlockSize();
	        long splitSize = computeSplitSize(blockSize, minSize, maxSize);

	        long bytesRemaining = length;
	        while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
	          int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
	          splits.add(new FileSplit(path, length-bytesRemaining, splitSize, 
	                                   blkLocations[blkIndex].getHosts()));
	          bytesRemaining -= splitSize;
	        }
	        
	        if (bytesRemaining != 0) {
	          splits.add(new FileSplit(path, length-bytesRemaining, bytesRemaining, 
	                     blkLocations[blkLocations.length-1].getHosts()));
	        }
	      } else if (length != 0) {
	        splits.add(new FileSplit(path, 0, length, blkLocations[0].getHosts()));
	      } else { 
	        //Create empty hosts array for zero length files
	        splits.add(new FileSplit(path, 0, length, new String[0]));
	      }
	    }
	    
	    // Save the number of input files in the job-conf
	    job.getConfiguration().setLong("mapreduce.input.num.files", files.size());

	    LOG.debug("Total # of splits: " + splits.size());
	    return splits;
	}

	protected long caculateSkipLength(FileStatus file,
			JobContext job) throws IOException,InterruptedException {
		FileSplit split = new FileSplit(file.getPath(), 0, file.getLen(),
				new String[0]);
		TDelimitedFileRecordReader<K,V> reader = (TDelimitedFileRecordReader<K,V>) getRecordReader(
				split, job);
		Text text = new Text();
		for (int i = 0; i < skipLines; i++) {
			reader.next(text);
		}
		return reader.getPos();
	}

	protected boolean isSplitable(FileSystem fs, Path filename) {
		CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(
				fs.getConf());
		CompressionCodec codec = compressionCodecs.getCodec(filename);
		if (codec != null) {
			return false;
		} else {
			return true;
		}
	}
}