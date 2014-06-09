package com.mogujie.app.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 
 * @author beifeng
 * @Date 2013-8-21 �????5:50:50
 * 
 */
public class CommonsUtils {

	public static void addTmpJar(Configuration conf) throws IOException {
		// String jarPath =
		// "/home/data/programs/hadoop-current/lib/hive-exec-0.9.0.jar";
		String jarPath = "/home/data/programs/hive/hive-0.12.0-cdh5.0.1/lib/hive-exec-0.12.0-cdh5.0.1.jar";
		System.setProperty("path.separator", ":");
		FileSystem fs = FileSystem.getLocal(conf);
		String newJarPath = new Path(jarPath).makeQualified(fs).toString();
		String tmpjars = conf.get("tmpjars");
		if (tmpjars == null || tmpjars.length() == 0) {
			conf.set("tmpjars", newJarPath);
		} else {
			conf.set("tmpjars", tmpjars + "," + newJarPath);
		}
	}

}
