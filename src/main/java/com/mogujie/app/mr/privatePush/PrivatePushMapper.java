/*
 * 蘑菇街 Inc.
 * Copyright (c) 2010-2014 All Rights Reserved.
 *
 * Author     :xingtian
 * Version    :1.0
 * Create Date:2014年4月20日
 */
package com.mogujie.app.mr.privatePush;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author xingtian
 * @version $Id: PrivatePushMapper.java,v 0.1 2014年4月20日 下午2:59:03 xingtian Exp
 *          $
 */

public class PrivatePushMapper extends
		Mapper<Object, Text, Text, BytesRefArrayWritable> {
	public static final String COLUMN_SPLIT = "\001";
	public static final String EMPTY_CHAR = "";
	public static final String KEY_SPLIT = ",";
	public static final String TAB = "\\t";

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		int size = context.getConfiguration().getInt(
				RCFile.COLUMN_NUMBER_CONF_STR, 0);
		String[] array = parse(value.toString());
		if (array == null) {
			return;
		}
		String[] valuesArray = array[0].split(COLUMN_SPLIT);
		if (valuesArray.length >= size) {
			BytesRefArrayWritable values = new BytesRefArrayWritable(size);
			for (int i = 0; i < size; i++) {
				values.set(i,
						new BytesRefWritable(valuesArray[i].getBytes("utf-8")));
			}
			context.write(new Text(array[1]), values);
		} else {
			BytesRefArrayWritable values = new BytesRefArrayWritable(size);
			for (int i = 0; i < valuesArray.length; i++) {
				values.set(i,
						new BytesRefWritable(valuesArray[i].getBytes("utf-8")));
			}
			for (int i = valuesArray.length; i < size; i++) {
				values.set(i, new BytesRefWritable(" ".getBytes("utf-8")));
			}
			context.write(new Text(array[1]), values);
		}
	}

	private String[] parse(String str) {
		StringBuilder sb = new StringBuilder(512);
		String split = COLUMN_SPLIT;
		Boolean record_status = true;
		String rub_visit_time = "2012-01-01 00:00:00";
		String visit_time = "";
		String visit_date = "";
		if (str.length() < 19 && str.indexOf("userId:") > 0
				&& str.indexOf(" to ") > 0 && str.indexOf(",") > 0) {
			record_status = false;
			visit_time = rub_visit_time;
			visit_date = "2012-01-01";
		} else {
			visit_time = str.substring(0, 19);
			SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			try {
				Date current_date = f.parse(visit_time);
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				visit_date = sdf.format(current_date);
			} catch (ParseException e) {
				record_status = false;
				visit_time = rub_visit_time;
				visit_date = "2012-01-01";
			}
		}
		String action = "";
		String type = "";
		String user_id = "";
		if (record_status) {
			String remain = str.substring(20);
			String[] pieces = remain.split(",");
			if (pieces.length > 1) {
				String[] actions = pieces[0].split(" to ");
				action = actions[0];
				if (actions.length > 1) {
					type = actions[1];
				}
			}
			int index = str.indexOf("userId:");
			if (index > 0) {
				user_id = str.substring(index + 7);
			}
		}
		sb.append(visit_time).append(split).append(action).append(split)
				.append(type).append(split).append(user_id);
		String[] result = new String[2];
		result[0] = sb.toString();
		result[1] = visit_date;
		return result;
	}

	public long string2long(String timeStamp) {
		timeStamp = timeStamp.replaceAll("[^\\d\\.]", "");
		if (timeStamp.length() <= 0) {
			return 0;
		}
		if (timeStamp.contains(".")) {
			int index = timeStamp.indexOf(".");
			timeStamp = timeStamp.substring(0, index);
		}
		return Long.parseLong(timeStamp);
	}

	public static void main(String args[]) throws Exception {
		PrivatePushMapper alm = new PrivatePushMapper();
		String str = "2014-04-20 00:07:10 sendDeliverPush to android, userId:73028824";
		String[] array = alm.parse(str);
		if (array == null) {
			System.out.println("213");
			return;
		}
		String[] pieces = array[0].split(COLUMN_SPLIT);
		for (String t : pieces) {
			System.out.println(t);
		}
	}
}
