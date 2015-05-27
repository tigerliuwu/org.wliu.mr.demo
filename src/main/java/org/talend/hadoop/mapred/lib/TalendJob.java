package org.talend.hadoop.mapred.lib;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Job.JobState;

import routines.system.MRRunStat;

public class TalendJob extends Job {
	  private JobState state = JobState.DEFINE;
	  private JobClient jobClient;
	  private RunningJob info;
	
	JobID jobId;
	
	
	public TalendJob(Configuration conf) throws IOException {
		super(conf);
	}

	public TalendJob(Configuration conf, String jobName) throws IOException {
		super(conf, jobName);
	}

	  private void ensureState(JobState state) throws IllegalStateException {
		    if (state != this.state) {
		      throw new IllegalStateException("Job in state "+ this.state + 
		                                      " instead of " + state);
		    }

		    if (state == JobState.RUNNING && jobClient == null) {
		      throw new IllegalStateException("Job in state " + JobState.RUNNING + 
		                                      " however jobClient is not initialized!");
		    }
		  }
	  private void ensureNotSet(String attr, String msg) throws IOException {
		    if (conf.get(attr) != null) {
		      throw new IOException(attr + " is incompatible with " + msg + " mode.");
		    }    
		  }
	  private void setUseNewAPI() throws IOException {
		    int numReduces = conf.getNumReduceTasks();
		    String oldMapperClass = "mapred.mapper.class";
		    String oldReduceClass = "mapred.reducer.class";
		    conf.setBooleanIfUnset("mapred.mapper.new-api",
		                           conf.get(oldMapperClass) == null);
		    if (conf.getUseNewMapper()) {
		      String mode = "new map API";
		      ensureNotSet("mapred.input.format.class", mode);
		      ensureNotSet(oldMapperClass, mode);
		      if (numReduces != 0) {
		        ensureNotSet("mapred.partitioner.class", mode);
		       } else {
		        ensureNotSet("mapred.output.format.class", mode);
		      }      
		    } else {
		      String mode = "map compatability";
		      ensureNotSet(JobContext.INPUT_FORMAT_CLASS_ATTR, mode);
		      ensureNotSet(JobContext.MAP_CLASS_ATTR, mode);
		      if (numReduces != 0) {
		        ensureNotSet(JobContext.PARTITIONER_CLASS_ATTR, mode);
		       } else {
		        ensureNotSet(JobContext.OUTPUT_FORMAT_CLASS_ATTR, mode);
		      }
		    }
		    if (numReduces != 0) {
		      conf.setBooleanIfUnset("mapred.reducer.new-api",
		                             conf.get(oldReduceClass) == null);
		      if (conf.getUseNewReducer()) {
		        String mode = "new reduce API";
		        ensureNotSet("mapred.output.format.class", mode);
		        ensureNotSet(oldReduceClass, mode);   
		      } else {
		        String mode = "reduce compatability";
		        ensureNotSet(JobContext.OUTPUT_FORMAT_CLASS_ATTR, mode);
		        ensureNotSet(JobContext.REDUCE_CLASS_ATTR, mode);   
		      }
		    }   
		  }
	  void setTalendJobID(JobID jobId1) {
		    jobId = jobId1;
	  }

	  /**
	   * Submit the job to the cluster and return immediately.
	   * @throws IOException
	   */
	  public void submit() throws IOException, InterruptedException, 
	                              ClassNotFoundException {
	    ensureState(JobState.DEFINE);
	    setUseNewAPI();
	    
	    // Connect to the JobTracker and submit the job
	    connect();
	    info = jobClient.submitJobInternal(conf);
	    setTalendJobID(info.getID());
	    state = JobState.RUNNING;
	   }
	  
	  /**
	   * Open a connection to the JobTracker
	   * @throws IOException
	   * @throws InterruptedException 
	   */
	  private void connect() throws IOException, InterruptedException {
	    ugi.doAs(new PrivilegedExceptionAction<Object>() {
	      public Object run() throws IOException {
	        jobClient = new JobClient((JobConf) getConfiguration());    
	        return null;
	      }
	    });
	  }
	  

	  
	  public RunningJob getRunningJob() {
		  return info;
	  }

}
