package org.wliu.mr2.factory;

import org.apache.hadoop.conf.Configuration;

public class MR2ConfigFactory {
	
	private static Configuration conf = null;
	
	static {
		init();
	}
	
	private static void init() {
		conf = new Configuration();
		conf.addResource("mr2config.xml");
	}
	
	public static Configuration getConfiguration() {
		if (conf == null) {
			init();
		}
		return conf;
	}

}
