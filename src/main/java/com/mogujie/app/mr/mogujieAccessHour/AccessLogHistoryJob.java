package com.mogujie.app.mr.mogujieAccessHour;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mogujie.app.mr.CommonsUtils;

/**
 * 
 * @author lvpinglin
 * @Date 2013-5-2 �????8:43:04
 * 
 */
public class AccessLogHistoryJob {

	public static void main(String[] args) throws Exception {
		System.out.println("-=-=-=-==-=-=-=");
		Configuration c = new Configuration();
		long milliSeconds = 1000 * 3600;
		c.setLong("mapred.task.timeout", milliSeconds);
		c.setInt(RCFile.COLUMN_NUMBER_CONF_STR,
				Integer.parseInt(args[args.length - 1]));
		CommonsUtils.addTmpJar(c);
		Job job = new Job(c, "access_log");

		job.setJarByClass(AccessLogHistoryJob.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesRefArrayWritable.class);
		job.setMapperClass(AccessLogHistoryMapper.class);
		job.setOutputFormatClass(AccessLogHistoryOutputFormat.class);
		for (int i = 0; i < args.length - 2; i++) {
			FileInputFormat.addInputPath(job, new Path(args[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 2]));
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}