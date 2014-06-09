/*
 * ??????�? Inc.
 * Copyright (c) 2010-2013 All Rights Reserved.
 *
 * Author     :yinxiu
 * Version    :1.0
 * Create Date:2013�?10???24???
 */
package com.mogujie.app.mr.tradetrace;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author yinxius
 * @version $Id: TradeClickTraceMapper.java,v 0.1 2013�?10???24??? �????1:31:14
 *          yinxiu Exp $
 */
public class TradeClickTraceMapper extends
		Mapper<Object, Text, Text, BytesRefArrayWritable> {

	private static final String TAB = "\t";
	public static final String KEY_SPLIT = ",";
	private static final SimpleDateFormat DATETIMEFORMAT = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	private static final SimpleDateFormat DATEFORMAT = new SimpleDateFormat(
			"yyyy-MM-dd");

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		int size = context.getConfiguration().getInt(
				RCFile.COLUMN_NUMBER_CONF_STR, 0);
		String[] array = parseLog(value.toString());
		String[] valueArray = array[0].split(TAB);
		if (valueArray.length >= size) {
			BytesRefArrayWritable values = new BytesRefArrayWritable(size);
			for (int i = 0; i < size; i++) {
				values.set(i,
						new BytesRefWritable(valueArray[i].getBytes("utf-8")));
			}
			context.write(new Text(array[1]), values);
		} else {
			BytesRefArrayWritable values = new BytesRefArrayWritable(size);
			for (int i = 0; i < valueArray.length; i++) {
				values.set(i,
						new BytesRefWritable(valueArray[i].getBytes("utf-8")));
			}
			for (int i = valueArray.length; i < size; i++) {
				values.set(i, new BytesRefWritable(" ".getBytes("utf-8")));
			}
			context.write(new Text(array[1]), values);
		}
	}

	private static String[] parseLog(String str) {
		String[] resultArray = new String[2];
		try {
			String serverTime = str.substring(0, str.indexOf(TAB)).trim();
			String logWithServerTime = str.substring(str.indexOf(TAB));
			String log = serverTime + logWithServerTime;
			Date logTime = DATETIMEFORMAT.parse(serverTime);
			long id = logTime.getTime() + Math.round(Math.random() * 1000);
			resultArray[0] = id + TAB + log;
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(logTime);
			resultArray[1] = DATEFORMAT.format(logTime) + KEY_SPLIT
					+ calendar.get(Calendar.HOUR_OF_DAY);
		} catch (Exception e) {
			e.printStackTrace();
			resultArray[0] = str;
			resultArray[1] = "2000-00-00,0";
		}
		return resultArray;
	}

	public static void main(String[] args) {
		String s = "2014-03-18 10:56:34 	8c7d042a-adf9-6675-fff4-faf96fa927c2	1		1	2	19		/cps/land/list/0?f=mgjlm	http://cps.namipan.cc/ad.php?adid=97			68743473";
		System.out.println(parseLog(s).length);
		String[] values = parseLog(s)[0].split(TAB);
		for (int i = 0; i < values.length; i++) {
			System.out.println(values[i]);
		}

	}
}
