package org.talend.hadoop.mapred.lib;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobClient.TaskStatusFilter;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.util.StringUtils;

import routines.system.MRRunStat;
import routines.system.MRRunStat.StatBean;

public class MRJobClient {
	protected final org.apache.hadoop.mapred.JobConf conf;
	private MRRunStat runStat;

	private int groupID;

	private int mrjobIDInGroup;
	
	public MRJobClient(Configuration config) {
		this.conf = new org.apache.hadoop.mapred.JobConf(config);
	}

	public void setRunStat(MRRunStat runStat) {
		this.runStat = runStat;
	}

	public void setGroupID(int groupID) {
		this.groupID = groupID;
	}

	public void setMRJobIDInGroup(int mrjobIDInGroup) {
		this.mrjobIDInGroup = mrjobIDInGroup;
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
		    }
		    if (numReduces != 0) {
		      conf.setBooleanIfUnset("mapred.reducer.new-api",
		                             conf.get(oldReduceClass) == null);
		      
		    }   
		  }
	

	public void runJob() throws IOException {
//		JobConf conf = (JobConf)job1.getConfiguration();
		setUseNewAPI();
		JobClient jc = new JobClient(conf);
		RunningJob job = jc.submitJob(conf);
//		RunningJob job = job1.getRunningJob();
		try {
			String lastReport = null;
			TaskStatusFilter filter;
			filter = JobClient.getTaskOutputFilter(conf);
			JobID jobId = job.getID();
			System.out.println("Running job: " + jobId);
			int eventCounter = 0;
			boolean profiling = conf.getProfileEnabled();
			Configuration.IntegerRanges mapRanges = conf
					.getProfileTaskRange(true);
			Configuration.IntegerRanges reduceRanges = conf
					.getProfileTaskRange(false);

		    /* make sure to report full progress after the job is done */
		    boolean reportedAfterCompletion = false;
		    
            while (!job.isComplete() || !reportedAfterCompletion) {
				if (job.isComplete()) {
				    reportedAfterCompletion = true;
				} else {
				    Thread.sleep(1000);
				}

				float mapProgress = job.mapProgress();
				float reduceProgress = job.reduceProgress();
				String report = (" map "
						+ StringUtils.formatPercent(mapProgress, 0)
						+ " reduce " + StringUtils.formatPercent(
						reduceProgress, 0));
				if (!report.equals(lastReport)) {
					System.out.println(report);
					lastReport = report;
				}
				if (runStat != null) {
					StatBean statBean = runStat.createStatBean();
					statBean.setGroupID(groupID);
					statBean.setMRJobID(mrjobIDInGroup);
					statBean.setMapProgress(mapProgress);
					statBean.setReduceProgress(reduceProgress);
					runStat.updateMRProgress(statBean);
				}

				TaskCompletionEvent[] events = job
						.getTaskCompletionEvents(eventCounter);
				eventCounter += events.length;

				for (TaskCompletionEvent event : events) {
					TaskCompletionEvent.Status status = event.getTaskStatus();
					if (profiling
							&& (status == TaskCompletionEvent.Status.SUCCEEDED || status == TaskCompletionEvent.Status.FAILED)
							&& (event.isMapTask() ? mapRanges : reduceRanges)
									.isIncluded(event.idWithinJob())) {
						URLConnection connection = new URL(
								event.getTaskTrackerHttp()
										+ "/tasklog?plaintext=true&attemptid="
										+ event.getTaskTrackerHttp()
										+ "&filter=profile").openConnection();
						InputStream in = connection.getInputStream();
						OutputStream out = new FileOutputStream(
								event.getTaskAttemptId() + ".profile");
						IOUtils.copyBytes(in, out, 64 * 1024, true);
					}
					switch (filter) {
					case NONE:
						break;
					case SUCCEEDED:
						if (event.getTaskStatus() == TaskCompletionEvent.Status.SUCCEEDED) {
							System.out.println(event.toString());
						}
						break;
					case FAILED:
						if (event.getTaskStatus() == TaskCompletionEvent.Status.FAILED) {
							System.out.println(event.toString());
							TaskAttemptID taskId = event.getTaskAttemptId();
							String[] taskDiagnostics = job
									.getTaskDiagnostics(taskId);
							if (taskDiagnostics != null) {
								for (String diagnostics : taskDiagnostics) {
									System.err.println(diagnostics);
								}
							}
						}
						break;
					case KILLED:
						if (event.getTaskStatus() == TaskCompletionEvent.Status.KILLED) {
							System.out.println(event.toString());
						}
						break;
					case ALL:
						System.out.println(event.toString());
						break;
					}
				}
			}
			System.out.println("Job complete: " + jobId);
			Counters counters = null;
			try {
				counters = job.getCounters();
			} catch (IOException ie) {
				counters = null;
				System.out.println(ie.getMessage());
			}
			if (counters != null) {
				System.out.println(counters);
			}
			if (!job.isSuccessful()) {
				System.err.println("Job Failed: " + job.getFailureInfo());
				throw new IOException("Job failed!");
			}
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		}
	}

}
