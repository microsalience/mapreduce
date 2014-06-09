package com.mogujie.app.mr;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author lvpinglin
 * @Date 2013-5-2 ä¸????8:42:10
 * 
 */

public class AccessLogReducer extends Reducer<Text,Text,Text,Text> {
	public static final Log LOG = LogFactory.getLog(AccessLogReducer.class);
	
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			context.write(value, key);
		}
	}

}
