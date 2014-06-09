package com.mogujie.app.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author lvpinglin
 * @Date 2013-5-2 ä¸????8:43:04
 * 
 */
public class AccessLogPerDayJob {

	public static void main(String[] args) throws Exception {
		System.out.println("-=-=-=-==-=-=-=");
		Configuration c = new Configuration();
		c.setInt(RCFile.COLUMN_NUMBER_CONF_STR, Integer.parseInt(args[2]));
		addTmpJar("/home/data/programs/hadoop-current/lib/hive-exec-0.9.0.jar",
				c);
		// c.set("mapreduce.map.output.textoutputformat.separator", "\\001");
		// c.set("mapred.textoutputformat.separator", "");
		Job job = new Job(c, "access_log");

		job.setJarByClass(AccessLogPerDayJob.class);

		job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(Text.class);
		job.setOutputValueClass(BytesRefArrayWritable.class);
		job.setMapperClass(AccessLogMapper.class);
		// job.setCombinerClass(AccessLogReducer.class);
		// job.setReducerClass(AccessLogReducer.class);
		// job.setOutputFormatClass(VVLogNameMultipleTextOutputFormat.class);
		job.setOutputFormatClass(RCFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	public static void addTmpJar(String jarPath, Configuration conf)
			throws IOException {
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