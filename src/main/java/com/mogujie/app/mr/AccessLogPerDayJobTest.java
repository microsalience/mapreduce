package com.mogujie.app.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author lvpinglin
 * @Date 2013-5-2 �????8:43:04
 * 
 */
public class AccessLogPerDayJobTest {

	public static void main(String[] args) throws Exception {
		System.out.println("-=-=-=-==-=-=-=");
		Configuration c = new Configuration();
		long milliSeconds = 1000 * 3600;
		c.setLong("mapred.task.timeout", milliSeconds);
		CommonsUtils.addTmpJar(c);
		// c.set("mapreduce.map.output.textoutputformat.separator", "\\001");
		// c.set("mapred.textoutputformat.separator", "");
		Job job = new Job(c, "access_log");

		job.setJarByClass(AccessLogPerDayJobTest.class);

		job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(AccessLogMapperTest.class);
		// job.setCombinerClass(AccessLogReducer.class);
		// job.setReducerClass(AccessLogReducer.class);
		// job.setOutputFormatClass(VVLogNameMultipleTextOutputFormat.class);
		job.setOutputFormatClass(VVLogNameMultipleTextOutputFormat.class);
		for (int i = 0; i < args.length - 1; i++) {
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
		// FileOutputFormat.setCompressOutput(job, true);
		// FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}