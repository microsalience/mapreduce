/*
 * ??????è¡? Inc.
 * Copyright (c) 2010-2014 All Rights Reserved.
 *
 * Author     :xingtian
 * Version    :1.0
 * Create Date:2014å¹?3???3???
 */
package com.mogujie.app.mr.mobileTrackDevice;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author xingtian
 * @version $Id: MobileTrackDeviceMapper.java,v 0.1 2014å¹?3???3??? ä¸????4:27:08 xingtian
 *          Exp $
 */
public class MobileTrackDeviceMapper extends
		Mapper<Object, Text, Text, BytesRefArrayWritable> {
	public static final String COLUMN_SPLIT = "\001";
	public static final String EMPTY_CHAR = "";
	public static final String KEY_SPLIT = ",";
	public static final String TAB = "\t";

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
		if (str.length() < 19) {
			record_status = false;
			visit_time = rub_visit_time;
		} else {
			visit_time = str.substring(0, 19);
			SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			try {
				f.parse(visit_time);
			} catch (ParseException e) {
				record_status = false;
				visit_time = rub_visit_time;
			}
		}
		int app_type = 0;
		String source = "";
		String version = "";
		String did = "";
		String device_type = "";
		String os_version = "";
		int is_root = 0;
		String size = "";
		String servers = "";
		int intenet = 0;
		String ip = "";
		String opentime = "";
		if (record_status) {
			String info = str.substring(20);
			String[] infoArr = info.split("\\|");
			if (infoArr.length != 12) {
				record_status = false;
			}
			if (record_status) {
				app_type = infoArr[0].length() > 0 ? Integer
						.parseInt(infoArr[0]) : 0;
				source = infoArr[1];
				version = infoArr[2];
				did = infoArr[3];
				device_type = infoArr[4];
				os_version = infoArr[5];
				is_root = infoArr[6].length() > 0 ? Integer
						.parseInt(infoArr[6]) : 0;
				size = infoArr[7];
				servers = infoArr[8];
				intenet = infoArr[9].length() > 0 ? Integer
						.parseInt(infoArr[9]) : 0;
				ip = infoArr[10];
				opentime = infoArr[11];
			} else {
				visit_time = str;
			}
		}
		String visit_date = "";
		visit_date = visit_time.substring(0, 10);
		sb.append(visit_time).append(split).append(app_type).append(split)
				.append(source).append(split).append(version).append(split)
				.append(did).append(split).append(device_type).append(split)
				.append(os_version).append(split).append(is_root).append(split)
				.append(size).append(split).append(servers).append(split)
				.append(intenet).append(split).append(ip).append(split)
				.append(opentime);
		String[] result = new String[2];
		result[0] = sb.toString();
		result[1] = visit_date;
		return result;
	}

	public static void main(String args[]) throws Exception {
		MobileTrackDeviceMapper alm = new MobileTrackDeviceMapper();
		String str = "2014-02-27 03:34:39 0|NIMDEV|5.4.0|31C38CB3-E6E4-48D8-ADED-E17C5E8B320A|iPhone5,2|7.0.4|0|1136*640|ä¸???½ç§»???|2|192.168.1.109|1393443279";
		String[] array = alm.parse(str);
		if (array == null) {
			System.out.println("213");
			return;
		}
		for (String s : array) {
			System.out.println(s);
		}
		String[] pieces = array[0].split("\001");
		for (String t : pieces) {
			System.out.println(t);
		}
	}
}
