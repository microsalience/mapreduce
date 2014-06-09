/*
 * ??????è¡? Inc.
 * Copyright (c) 2010-2013 All Rights Reserved.
 *
 * Author     :yinxiu
 * Version    :1.0
 * Create Date:2013å¹?10???25???
 */
package com.mogujie.app.mr.cnjie;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author yinxiu
 * @version $Id: NignxAccessCNjieMapper.java,v 0.1 2013å¹?10???25??? ä¸????2:05:12 yinxiu
 *          Exp $
 */
public class NginxAccessCNjieMapper extends
		Mapper<Object, Text, Text, BytesRefArrayWritable> {
	public static final String COLUMN_SPLIT = "\001";
	public static final String EMPTY_CHAR = "";
	public static final String KEY_SPLIT = ",";
	private static final SimpleDateFormat DATETIMEFORMAT = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");
	private static final SimpleDateFormat DATEFORMAT = new SimpleDateFormat(
			"yyyy-MM-dd");

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		int size = context.getConfiguration().getInt(
				RCFile.COLUMN_NUMBER_CONF_STR, 0);
		String[] array = parseLog(value.toString());
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

	private String[] parseLog(String log) {
		String[] resultArray = new String[2];
		try {
			StringBuilder sb = new StringBuilder(512);
			String split = COLUMN_SPLIT;
			// è§£æ??
			String ip = regexp_extract(log, "(\\d+.\\d+.\\d+.\\d+)(.*)", 1);
			String visit_time = regexp_extract(log,
					"( - - \\[)(.*)( \\+0800\\].*)", 2);
			String url = regexp_extract(log, "([^\"]*\"){1}([^\"]*)", 2).split(
					" ")[1];
			String outer_code = regexp_extract(log,
					"( \".*&id=)([^&\\s]*)(&.*)", 2);
			String tid = regexp_extract(log, "( \".*&tid=)([^&\\s]*)(&.*)", 2);
			String iid = regexp_extract(log, "( \".*&iid=)([^&\\s]*)(&.*)", 2);
			String uid = regexp_extract(log, "( \".*&uid=)([^&\\s]*)(&.*)", 2);
			String cfav = regexp_extract(log, "( \".*&cfav=)([^&\\s]*)(&.*)", 2);
			String creply = regexp_extract(log,
					"( \".*&creply=)([^&\\s]*)(&.*)", 2);
			String ff = regexp_extract(log, "( \".*&ff=)([^&\\s]*)(&.*)", 2);
			String sfrom = regexp_extract(log, "( \".*&sfrom=)([^&\\s]*)(&.*)",
					2);
			String ma = regexp_extract(log, "( \".*&ma=)([^&\\s]*)(&.*)", 2);
			String uuid = regexp_extract(log, "( \".*&uuid=)([^&\\s]*)(&.*)", 2);
			String from_ = regexp_extract(log, "( \".*&from=)([^&\"\\s]*)(.*)",
					2);
			String protocol = regexp_extract(log,
					"( \".*HTTP/)(\\d+.\\d+)(\" \\d+ \\d+.*)", 2);
			String status = regexp_extract(log,
					"( \".*HTTP/\\d+.\\d+\" )(\\d+ \\d+)( \".*)", 2);
			String did = regexp_extract(log, "( \".*&_did=)([^&\"\\s]*)(.*)", 2);
			String ttid = regexp_extract(log, "( \".*&ttid=)([^&\"\\s]*)(.*)",
					2);
			String agent = regexp_extract(log,
					"( \"GET .*\" \\d+ \\d+ \".*\" \")(.*\\(.*)", 2);
			String refer = "-";
			if (log.indexOf("jquery") < 0) {
				refer = regexp_extract(log,
						"( \"GET .*\" \\d+ \\d+ \")(.*)(\" \".*\\(.*)", 2);
			}
			// è½????
			url = UrlDecode(url);
			refer = UrlDecode(refer);
			tid = parseUserid(tid);
			String goodsid = parseUserid(iid);
			String userid = parseUserid(uid);
			cfav = parseUserid(cfav);
			creply = parseUserid(creply);
			ff = UrlDecode(ff);
			sfrom = UrlDecode(sfrom);
			SimpleDateFormat f = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",
					Locale.ENGLISH);
			Date date = f.parse(visit_time);
			visit_time = DATETIMEFORMAT.format(date);
			// ?????? id
			String id = ""
					+ (date.getTime() + Math.round(Math.random() * 1000));
			// è½????
			sb.append(id).append(split).append(ip).append(split)
					.append(visit_time).append(split).append(url).append(split)
					.append(outer_code).append(split).append(tid).append(split)
					.append(goodsid).append(split).append(userid).append(split)
					.append(cfav).append(split).append(creply).append(split)
					.append(ff).append(split).append(sfrom).append(split)
					.append(uuid).append(split).append(from_).append(split)
					.append(ma).append(split).append(protocol).append(split)
					.append(status).append(split).append(refer).append(split)
					.append(agent).append(split).append(did).append(split)
					.append(ttid);
			resultArray[0] = sb.toString();
			// ???å®?è®°å?????å±???????
			Date logTime = DATETIMEFORMAT.parse(visit_time);
			Calendar calendar = Calendar.getInstance();
			resultArray[1] = DATEFORMAT.format(logTime) + KEY_SPLIT
					+ calendar.get(Calendar.HOUR_OF_DAY);
		} catch (Exception e) {
			e.printStackTrace();
			resultArray[0] = log;
			resultArray[1] = "2000-00-00,0";
		}
		return resultArray;
	}

	private String regexp_extract(String log, String pattern, int number) {
		Pattern p = Pattern.compile(pattern);
		Matcher m = p.matcher(log);

		if (m.find()) {
			if (m.groupCount() < number) {
				return null;
			} else {
				return m.group(number);
			}
		} else {
			return null;
		}
	}

	private String parseUserid(String uid) {
		// uid ä¸ºç©º??? ??¿åº¦å°?äº?2??? è¿????ç©ºå??ç¬?ä¸?
		if (StringUtils.isEmpty(uid) || uid.length() < 2) {
			return EMPTY_CHAR;
		}
		String strUserid = uid.substring(1);
		long userid = 0;
		// ä»?36è¿???¶ç??å­?ç¬?ä¸?str??????ä¸?ä¸?BigInteger
		BigInteger big = new BigInteger(strUserid, 36);
		// å°?è¿?ä¸?36è¿???¶è½¬???ä¸?10è¿???¶è¡¨ç¤ºç??å­?ç¬?ä¸?
		String strUserid10 = big.toString(10);
		// è®¡ç??userid
		userid = (Long.parseLong(strUserid10) - 56) / 2;
		return String.valueOf(userid);
	}

	private String UrlDecode(String s) throws Exception {
		if (StringUtils.isEmpty(s)) {
			return EMPTY_CHAR;
		}
		String src_url = s.toString();
		String url = EMPTY_CHAR;
		try {
			url = URLDecoder.decode(
					URLDecoder.decode(src_url.replace("+", "%2b"), "UTF-8")
							.replace("+", "%2b"), "UTF-8");
		} catch (Exception e) {
			URLDecoder.decode("", "UTF-8");
			url = src_url.replace("%3A", ":").replace("%2F", "/");
		}
		return url;
	}

	public static void main(String args[]) throws Exception {
		NginxAccessCNjieMapper alm = new NginxAccessCNjieMapper();
		String log = "27.18.58.139 - - [09/Feb/2014:21:48:29 +0800] \"GET /tgo/?to=aHR0cDovL2l0ZW0udGFvYmFvLmNvbS9pdGVtLmh0bT9pZD0zNzEwNTc5ODkxOA&id=9H7PZTsLHy&tid=0&iid=0&uid=11aysw&ff=http%3A%2F%2Fwww.mogujie.com%2Fbook%2Fclothing%2F9532%2F%3FfId%3D1jr13ri%26from%3Dhpc_5&sfrom=stats.b5m.com%5Emagicindexstream&ma=%3D%3DgMxgDOxcDO&uuid=-08fd-00-08fd-3418116c2418116c2418f9&ocb=600432480&valid=5ull2601c1d645171ne499f5b2f26ae84de3bf26&from=image-note-item&itemId=120b38w HTTP/1.1\" 302 5 \"http://www.mogujie.com/note/1juypdk?showtype=imageitem&itemtid=1jvmsbk&traceid=book_yekt2c2afpc\" \"Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1\" 27.18.58.139 \"\" 0.013 0.013 \"\" \"\" \"juanniu4069\" \"mogujie.cn\" \"##\" \"5\" \"0.0008\" \"-\" \"-\"";
		String[] array = alm.parseLog(log);
		String[] ss = array[0].split(COLUMN_SPLIT);
		for (String s : ss) {
			System.out.println(s);
		}
		System.out.println(array[1]);
	}
}
