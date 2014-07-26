package org.talend.samples;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.talend.map.input.Row1StructInputFormat;
import org.talend.map.input.row1Struct;
import org.talend.map.output.row3Struct;
import org.talend.map.output.tHDFSOutput_1StructOutputFormat;

/**
 * @author wliu
 *
 */
public class MapperContextMain extends Configured implements Tool {
	

	private java.util.Properties context_param = new java.util.Properties();
	public java.util.Map<String, Object> parentContextMap = new java.util.HashMap<String, Object>();
	public String contextStr = "Default";
	public boolean isDefaultContext = true;
	
	
	private final static String jobVersion = "0.1";
	private final static String jobName = "Test";
	private final static String projectName = "AAA";
	
	String mr_temp_dir= "";
	
	// create and load default properties
	private java.util.Properties defaultProps = new java.util.Properties();
	private GlobalVar globalMap = null;
	// create application properties with default
	public static class ContextProperties extends java.util.Properties {

		private static final long serialVersionUID = 1L;

		public static final String CONTEXT_FILE_NAME = "talend.context.fileName";
		public static final String CONTEXT_KEYS = "talend.context.keys";
		public static final String CONTEXT_PARAMS_PREFIX = "talend.context.params.";
		public static final String CONTEXT_PARENT_KEYS = "talend.context.parent.keys";
		public static final String CONTEXT_PARENT_PARAMS_PREFIX = "talend.context.parent.params.";

		public ContextProperties(java.util.Properties properties) {
			super(properties);
		}

		public ContextProperties() {
			super();
		}

		public ContextProperties(Configuration job) {
			super();
			String contextFileName = job.get(CONTEXT_FILE_NAME);
			try {
				if (contextFileName != null && !"".equals(contextFileName)) {
					java.io.File contextFile = new java.io.File(contextFileName);
					if (contextFile.exists()) {
						java.io.InputStream contextIn = contextFile.toURI()
								.toURL().openStream();
						this.load(contextIn);
						contextIn.close();
					} else {

						java.io.InputStream contextIn = MapperContextMain.class
								.getClassLoader().getResourceAsStream(
										"contexts/"
												+ contextFileName);
						if (contextIn != null) {
							this.load(contextIn);
							contextIn.close();
						}
					}
				}
				java.util.StringTokenizer st = new java.util.StringTokenizer(
						job.get(CONTEXT_KEYS, ""), " ");
				while (st.hasMoreTokens()) {
					String contextKey = st.nextToken();
					if (job.get(CONTEXT_PARAMS_PREFIX + contextKey) != null) {
						this.put(contextKey,
								job.get(CONTEXT_PARAMS_PREFIX + contextKey));
					}
				}
				st = new java.util.StringTokenizer(job.get(CONTEXT_PARENT_KEYS,
						""), " ");
				while (st.hasMoreTokens()) {
					String contextKey = st.nextToken();
					if (job.get(CONTEXT_PARENT_PARAMS_PREFIX + contextKey) != null) {
						this.put(
								contextKey,
								job.get(CONTEXT_PARENT_PARAMS_PREFIX
										+ contextKey));
					}
				}

				this.loadValue(null, job);
			} catch (java.io.IOException ie) {
				System.err.println("Could not load context " + contextFileName);
				ie.printStackTrace();
			}
		}

		public void synchronizeContext() {
			if (name != null) {
				this.setProperty("name", name.toString());
			}
		}

		public String name;

		public String getName() {
			return this.name;
		}

		public void loadValue(java.util.Properties context_param,
				Configuration job) {

			this.name = (String) this.getProperty("name");

		}
	}
	
	private static class GlobalVar {
		public static final String GLOBALVAR_PARAMS_PREFIX = "talend.globalvar.params.";
		private Configuration job;
		private java.util.Map<String, Object> map;

		public GlobalVar(Configuration job) {
			this.job = job;
			this.map = new java.util.HashMap<String, Object>();
		}

		public Object get(String key) {
			String tempValue = job.get(GLOBALVAR_PARAMS_PREFIX + key);
			if (tempValue != null) {
				return SerializationUtils.deserialize(Base64
						.decodeBase64(StringUtils.getBytesUtf8(tempValue)));
			} else {
				return null;
			}
		}

		public void put(String key, Object value) {
			if (value == null)
				return;
			job.set(GLOBALVAR_PARAMS_PREFIX + key, StringUtils
					.newStringUtf8(Base64.encodeBase64(SerializationUtils
							.serialize((Serializable) value))));
		}

		public void putLocal(String key, Object value) {
			map.put(key, value);
		}

		public Object getLocal(String key) {
			return map.get(key);
		}
	}

	private ContextProperties context = new ContextProperties();
	
	private void initContext() {
		// get context
		try {
			// call job/subjob with an existing context, like:
			// --context=production. if without this parameter, there will use
			// the default context instead.
			java.io.InputStream inContext = MapperContextMain.class.getClassLoader()
					.getResourceAsStream(
							"contexts/" + contextStr
									+ ".properties");
			if (isDefaultContext && inContext == null) {

			} else {
				if (inContext != null) {
					// defaultProps is in order to keep the original context
					// value
					defaultProps.load(inContext);
					inContext.close();
					context = new ContextProperties(defaultProps);
				} else {
					// print info and job continue to run, for case:
					// context_param is not empty.
					System.err.println("Could not find the context "
							+ contextStr);
				}
			}

			if (!context_param.isEmpty()) {
				context.putAll(context_param);
			}
			context.loadValue(context_param, null);
			if (parentContextMap != null && !parentContextMap.isEmpty()) {

				if (parentContextMap.containsKey("name")) {
					context.name = (String) parentContextMap.get("name");
				}

			}
		} catch (java.io.IOException ie) {
			System.err.println("Could not load context " + contextStr);
			ie.printStackTrace();
		}
	}
	private void setContext(Configuration conf) {
		// get context
		try {
			// call job/subjob with an existing context, like:
			// --context=production. if without this parameter, there will use
			// the default context instead.
			java.net.URL inContextUrl = MapperContextMain.class.getClassLoader()
					.getResource(
							"contexts/" + contextStr
									+ ".properties");
			if (isDefaultContext && inContextUrl == null) {

			} else {
				if (inContextUrl != null) {
					conf.set(ContextProperties.CONTEXT_FILE_NAME, contextStr
							+ ".properties");
					DistributedCache.addCacheFile(
							new java.net.URI(inContextUrl.getProtocol(),
									inContextUrl.getHost(), inContextUrl
											.getPath(), contextStr
											+ ".properties"), conf);
				}
			}

			if (!context_param.isEmpty()) {
				for (Object contextKey : context_param.keySet()) {
					conf.set(ContextProperties.CONTEXT_PARAMS_PREFIX
							+ contextKey,
							context.getProperty(contextKey.toString()));
					conf.set(ContextProperties.CONTEXT_KEYS,
							conf.get(ContextProperties.CONTEXT_KEYS, "") + " "
									+ contextKey);
				}
			}

			if (parentContextMap != null && !parentContextMap.isEmpty()) {

				if (parentContextMap.containsKey("name")) {
					conf.set(ContextProperties.CONTEXT_PARENT_PARAMS_PREFIX
							+ "name", parentContextMap.get("name").toString());
					conf.set(ContextProperties.CONTEXT_PARENT_KEYS,
							conf.get(ContextProperties.CONTEXT_KEYS, "") + " "
									+ "name");
				}

			}
		} catch (java.net.URISyntaxException e) {
			System.err.println("Could not load context " + contextStr);
			e.printStackTrace();
		}
	}
	private void initMapReduceJob(Configuration conf) {
		// set basic info
		FileSystem.setDefaultUri(conf, "hdfs://wliu-work:9000");

		conf.set("mapred.job.tracker", "wliu-work:9001");



		// set context
		setContext(conf);


	}
	
	private void validTempFolder(String mr_temp_dir) throws Exception {
		java.io.File[] rootFoldersArray = java.io.File.listRoots();
		java.util.List<java.io.File> listRootFoldersReadOnly = java.util.Arrays
				.asList(rootFoldersArray);
		java.util.List<java.io.File> listRootFolders = new java.util.ArrayList<java.io.File>(
				listRootFoldersReadOnly);
		listRootFolders.add(new java.io.File(System.getProperty("user.home")));

		listRootFolders.add(new java.io.File("/user/" + "wliu"));

		listRootFolders.add(new java.io.File("/user/"
				+ System.getProperty("user.name")));
		if (listRootFolders.contains(new java.io.File(mr_temp_dir))) {
			throw new Exception(
					"Using a root folder or a home folder as the temporary directory is not recommended, please choose another one.");
		}
	}
	
	private void clearTempFolder() {
		try {
	    // remove the temp dirs
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(new Path(mr_temp_dir), true);
		} catch(Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public static class SimpleMapper extends  Mapper<NullWritable, row1Struct, NullWritable, row3Struct> {
		Log log = LogFactory.getLog(getClass());
		ContextProperties context;
		GlobalVar globalMap;
		row3Struct outValue = new row3Struct();
		
		protected void setup(Context mrContext) throws IOException, InterruptedException {
			this.context = new ContextProperties(mrContext.getConfiguration());
			this.globalMap = new GlobalVar(mrContext.getConfiguration());
		}
		
		protected void map(NullWritable key, row1Struct value, 
	              Context mrContext) throws IOException, InterruptedException {
			 if (value.sex!=null && value.sex.toLowerCase().equals("male")) {
				 outValue.name = value.name;
				 outValue.ID= value.ID;
				 outValue.age = value.age;
				 System.out.println("======map context.name=" + context.name +  "=============");
				 log.info("======map log context.name=" + context.name +  "=============");
				 mrContext.write(NullWritable.get(), outValue);
			 }
		  }
	}

	public int run(String[] args) throws Exception {
		
		initContext();
		
		initMapReduceJob(getConf());
		
		globalMap = new GlobalVar(getConf());
		
		mr_temp_dir = (new java.io.File("/tmp", jobName)).toString();
		validTempFolder(mr_temp_dir);
		
		
		String input = "/user/wliu/multiple/input/in/in1.txt";
		String output = "/user/wliu/multiple/out3/out2";
		Configuration conf = getConf();
		

	    
//	    FileSystem.setDefaultUri(conf, "hdfs://wliu-work:9000");
//	    conf.set("mapred.job.tracker", "wliu-work:9001");
	     
	    // remove the output directory
	    FileSystem fs = FileSystem.get(conf);
	    Path out = new Path(output);

	    Job job = new Job(conf,this.getClass().getCanonicalName());

	    job.setJarByClass(MapperContextMain.class);
	    
	    System.out.println("=======in the main part, context.name="+context.name + "===============");
	    
	    
	    
	    job.setMapperClass(SimpleMapper.class);
	    
	    job.setInputFormatClass(Row1StructInputFormat.class);
	    Row1StructInputFormat.setInputPaths(job, new Path(input));
	    
	    job.setOutputFormatClass(tHDFSOutput_1StructOutputFormat.class);
	    tHDFSOutput_1StructOutputFormat.setOutputPath(job, out);
	    
	    fs.delete(out, true);
	    
	    job.setNumReduceTasks(0);

//	    System.exit(job.waitForCompletion(true)?0:1);
	    int res = job.waitForCompletion(true)?0:1;
		
	    // remove the temp dirs
		clearTempFolder();
		
	    return res;
	  }

	  public static void main(String[] args) throws Exception {

	    int res = ToolRunner.run(new Configuration(), new MapperContextMain(), args);
	    System.exit(res); 
	  }

}
