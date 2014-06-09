/*
 * ??????�? Inc.
 * Copyright (c) 2010-2013 All Rights Reserved.
 *
 * Author     :yinxiu
 * Version    :1.0
 * Create Date:2013�?10???24???
 */
package com.mogujie.app.mr.tradetrace;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mogujie.app.mr.CommonsUtils;
import com.mogujie.app.mr.GeneralLogFile;

/**
 * @author yinxiu
 * @version $Id: TradeClickTraceJob.java,v 0.1 2013�?10???24??? �????3:23:38
 *          yinxiu Exp $
 */
public class TradeClickTraceJob {
	private static final String HDFS_URI = "hdfs://qihe2061:8020";
	private static final String JobName = "trade_click_trace";

	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			DistributedFileSystem dfs = new DistributedFileSystem();
			dfs.initialize(new URI(HDFS_URI), conf);
			List<GeneralLogFile> cFiles = new ArrayList<GeneralLogFile>();
			for (int i = 0; i < args.length - 2; i++) {
				cFiles.add(new GeneralLogFile(args[i], "Y"));
			}
			if (cFiles.size() <= 0) {
				System.out.println("---" + JobName + " complete---");
				dfs.close();
				return;
			}
			Configuration c = new Configuration();
			c.set("mapred.job.priority", JobPriority.VERY_HIGH.name());
			long milliSeconds = 1000 * 3600;
			c.setLong("mapred.task.timeout", milliSeconds);
			c.setInt(RCFile.COLUMN_NUMBER_CONF_STR,
					Integer.parseInt(args[args.length - 1]));
			CommonsUtils.addTmpJar(c);
			Job job = new Job(c, JobName);
			job.setJarByClass(TradeClickTraceJob.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(BytesRefArrayWritable.class);
			job.setMapperClass(TradeClickTraceMapper.class);
			job.setOutputFormatClass(TradeClickTraceOutputFormat.class);
			for (GeneralLogFile file : cFiles) {
				FileInputFormat.addInputPath(job, new Path(file.getFilePath()));
			}
			FileOutputFormat
					.setOutputPath(job, new Path(args[args.length - 2]));
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
			job.waitForCompletion(true);
			dfs.close();
			System.out.println("---" + JobName + " complete---");
		} catch (Exception e) {
			System.out.println("Error!!!!!!!!!!!!!!!!!!!!!");
			e.printStackTrace();
		}
	}
}
