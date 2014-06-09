/*
 * 蘑菇街 Inc.
 * Copyright (c) 2010-2014 All Rights Reserved.
 *
 * Author     :xingtian
 * Version    :1.0
 * Create Date:2014年5月23日
 */
package com.mogujie.app.mr.bookLog;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author xingtian
 * @version $Id: BookLogMapper.java,v 0.1 2014年5月23日 下午3:31:06 xingtian Exp $
 */
public class BookLogMapper extends
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
		String[] returnArray = new String[2];
		try {
			StringBuilder sb = new StringBuilder(512);
			String split = COLUMN_SPLIT;
			String[] array = str.split("\"");
			String first = array[0];
			String visit_date = getDate(first);
			String ip = first.substring(0, first.indexOf("-")).trim();
			Long ipLong = ipToNum(ip);
			String request = array[1].split(" ")[1].trim();
			String url = parseParam(request, "pageurl");
			String screenwidth = parseParam(request, "screenwidth");
			String browser = parseParam(request, "broswer");
			String errors = parseParam(request, "errors");
			String wallload = parseParam(request, "wallload");
			String wallclick = parseParam(request, "wallclick");
			String netspeed = parseParam(request, "netspeed");
			String staytime = parseParam(request, "staytime");
			String uuid = parseParam(request, "uuid");
			String action = parseParam(request, "action");
			String refer = array[3].trim();
			String httpStatus = array[2].trim();
			String client = array[5].trim();
			String extra_param = "";
			sb.append(ip).append(split).append(ipLong).append(split)
					.append(url).append(split).append(screenwidth)
					.append(split).append(browser).append(split).append(errors)
					.append(split).append(wallload).append(split)
					.append(wallclick).append(split).append(netspeed)
					.append(split).append(staytime).append(split).append(uuid)
					.append(split).append(action).append(split).append(refer)
					.append(split).append(httpStatus).append(split)
					.append(client).append(split).append(extra_param);
			returnArray[0] = sb.toString();
			returnArray[1] = visit_date;
		} catch (Exception e) {
			e.printStackTrace();
			returnArray[0] = str;
			returnArray[1] = "2000-00-00,0";
		}
		return returnArray;
	}

	private String parseParam(String request, String matcher) {
		String result = EMPTY_CHAR;
		String sep1 = "&";
		String sep2 = "?";
		String tempMatcher = sep1 + matcher + "=";
		int index = request.indexOf(tempMatcher);
		if (index > 0) {
			result = cleanString(request.substring(index + tempMatcher.length()));
			return result;
		}
		tempMatcher = sep2 + matcher + "=";
		index = request.indexOf(tempMatcher);
		if (index > 0) {
			result = cleanString(request.substring(index + tempMatcher.length()));
		}
		return result;
	}

	private String cleanString(String str) {
		String returnValue = EMPTY_CHAR;
		int index = str.indexOf("&");
		if (index >= 0) {
			returnValue = str.substring(0, index);
		} else {
			returnValue = str;
		}
		return returnValue;
	}

	public String getDate(String initDate) throws ParseException {
		initDate = initDate.substring(initDate.indexOf("[") + 1,
				initDate.indexOf("+")).trim();
		SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat f = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",
				Locale.ENGLISH);
		Date date = new Date();
		date = f.parse(initDate);
		return dateFormatter.format(date);
	}

	private long ipToNum(String ip) {
		long returnValue = 0;
		try {
			long num = 0;
			String[] sections = ip.split("\\.");
			int i = 3;
			for (String str : sections) {
				num += (Long.parseLong(str) << (i * 8));
				i--;
			}
			returnValue = num;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return returnValue;
	}

	public static void main(String args[]) throws Exception {
		BookLogMapper alm = new BookLogMapper();
		//String str = "10.11.5.155 - [23/May/2014:15:43:30 +0800] \"GET /mogu?pageurl=http://www.mogujie.com/book/clothing/?from=hpc_1&ad=1&f=mgjstat2345m&screenwidth=1423&staytime=132s&broswer=IE7.0&errors=&wallload=p0:5860ms&uuid=f7282020-89d4-15ad-4b87-321433562cc21400866923093&action=entry HTTP/1.1\" 200 \"http://www.mogujie.com/book/clothing/?from=hpc_1&ad=1&f=mgjstat2345m\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.04506.648; .NET CLR 3.5.21022; InfoPath.3; 2345Explorer 3.2.0.12012)\"";
		String str = "183.24.97.35 - [29/May/2014:16:18:05 +0800] \"GET /mogu?pageurl=http://www.mogujie.com/book/shoes/9817/?from=hpc_6&screenwidth=1265&broswer=Safari537.36&errors=&wallload=p0:1803ms,p2:1148ms,p3:276ms,p4:611ms,p5:220ms,p6:2425ms,p7:275ms,p8:254ms,p9:2039ms,p10:830ms,p11:3641ms,p12:398ms&wallclick=1kn2gw2&netspeed=s13:9ms,s6:8ms,s12:7ms,s7:7ms&staytime=197s&uuid=c626da78-52e4-3084-8d0c-5e0000ab591f1401351284418&action=leave HTTP/1.1\" 200 \"http://www.mogujie.com/book/shoes/9817/?from=hpc_6\" \"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.117 Safari/537.36\"";
		String[] array = alm.parse(str);
		String[] pieces = array[0].split("\001");
		for (String t : pieces) {
			System.out.println(t);
		}
	}
}
