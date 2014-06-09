package com.mogujie.app.mr.mogujieAccessHour;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author lvpinglin
 * @Date 2013-5-2 �????7:23:35
 * 
 */
public class AccessLogHistoryMapper extends
		Mapper<Object, Text, Text, BytesRefArrayWritable> {

	public static final String COLUMN_SPLIT = "\001";
	public static final String EMPTY_CHAR = "";
	public static final String KEY_SPLIT = ",";

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		int size = context.getConfiguration().getInt(
				RCFile.COLUMN_NUMBER_CONF_STR, 0);
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		String visit_date = formatter.format(new Date());
		BytesRefArrayWritable values = new BytesRefArrayWritable(size);
		values.set(0, new BytesRefWritable(value.toString().getBytes("UTF-8")));
		context.write(new Text(visit_date), values);
	}

}
