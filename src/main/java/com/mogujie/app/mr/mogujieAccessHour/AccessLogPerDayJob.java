package com.mogujie.app.mr.mogujieAccessHour;

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
 * 
 * @author lvpinglin
 * @Date 2013-5-2 �????8:43:04
 * 
 */
public class AccessLogPerDayJob {
	private static final String HDFS_URI = "hdfs://qihe2061:8020";
	private static final String JobName = "mogujie_access_hour";

	public static void main(String[] args) throws Exception {
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
			job.setJarByClass(AccessLogPerDayJob.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(BytesRefArrayWritable.class);
			job.setMapperClass(AccessLogMapper.class);
			job.setOutputFormatClass(AccessLogOutputFormat.class);
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