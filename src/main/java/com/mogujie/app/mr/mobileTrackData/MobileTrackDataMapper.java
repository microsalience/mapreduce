/*
 * ??????�? Inc.
 * Copyright (c) 2010-2014 All Rights Reserved.
 *
 * Author     :xingtian
 * Version    :1.0
 * Create Date:2014�?3???3???
 */
package com.mogujie.app.mr.mobileTrackData;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author xingtian
 * @version $Id: MobileTrackDataMapper.java,v 0.1 2014�?3???3??? �????5:16:39
 *          xingtian Exp $
 */
public class MobileTrackDataMapper extends
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
		if (str.length() <= 0) {
			return null;
		}
		String data_type = "";
		String did = "";
		String opentime = "";
		String event = "";
		String userid = "";
		String trace_desc = "";
		String event_code = "";
		String clienttime = "";
		String detail = "";
		Long servertime = (long) 0;
		String[] infoArr = str.split(TAB);
		String visit_date = "";
		String[] result = new String[2];
		if (infoArr.length >= 9) {
			data_type = infoArr[0];
			did = infoArr[1];
			opentime = infoArr[2];
			event = infoArr[3];
			userid = infoArr[4];
			trace_desc = infoArr[5];
			event_code = infoArr[6];
			clienttime = infoArr[7];
			servertime = string2long(infoArr[8]);
			if (infoArr.length >= 10) {
				detail = infoArr[9];
			}
			Long current = System.currentTimeMillis();
			Long distance = current - servertime * 1000L;
			// 15天前 1天后的数据抛弃
			if (distance > 86400 * 15 * 1000L || distance < -86400 * 1000L) {
				visit_date = "2012-01-01";
			} else {
				Date date = new Date(servertime * 1000L);
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				visit_date = sdf.format(date);
			}
		} else {
			data_type = str;
			visit_date = "2012-01-01";
		}
		sb.append(data_type).append(split).append(did).append(split)
				.append(opentime).append(split).append(event).append(split)
				.append(userid).append(split).append(trace_desc).append(split)
				.append(event_code).append(split).append(clienttime)
				.append(split).append(servertime).append(split).append(detail);
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
		MobileTrackDataMapper alm = new MobileTrackDataMapper();
//		String str = "e	31C38CB3-E6E4-48D8-ADED-E17C5E8B320A	1398218303	816			108	1398218798	1398218798	{\"iid\" : \"16mjg0o\"}";
		String str = "e       59593195-2696-42DC-9635-3FB09E10560F    1398236260      2904    12i5ojw         xz002   1398236727      1398236726      {\"iid\" : \"16mx864\"}";
		// String str =
		// "p	14:8F:C6:B2:CA:69	6034065.09	0	 	YoudianDetail	02	6035836.65	1394570335.6%";
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
