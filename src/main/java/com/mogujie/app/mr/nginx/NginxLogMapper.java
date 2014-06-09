package com.mogujie.app.mr.nginx;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author lvpinglin
 * @Date 2013-5-2 ä¸????7:23:35
 * 
 */
public class NginxLogMapper extends
		Mapper<Object, Text, Text, BytesRefArrayWritable> {

	public static final String COLUMN_SPLIT = "\001";
	public static final String EMPTY_CHAR = "";

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		int size = context.getConfiguration().getInt(
				RCFile.COLUMN_NUMBER_CONF_STR, 0);
		String[] array = parseUrl(value.toString());
		String[] valuesArray = array[0].split(COLUMN_SPLIT);
		if (valuesArray.length == size) {
			BytesRefArrayWritable values = new BytesRefArrayWritable(size);
			for (int i = 0; i < size; i++) {
				values.set(i,
						new BytesRefWritable(valuesArray[i].getBytes("utf-8")));
			}
			context.write(new Text(array[1]), values);
		}
	}

	private String[] parseUrl(String str) {
		String[] returnArray = new String[2];
		try {
			StringBuilder sb = new StringBuilder(512);
			String split = COLUMN_SPLIT;
			String[] array = str.split("\"");
			String first = array[0].trim();
			int indexThree = first.indexOf("-");
			String ip = first.substring(0, indexThree).trim();
			int indexOne = first.indexOf("[");
			String remote_user = first.substring(indexThree+1, indexOne).trim();
			String request = array[1].split(" ")[1].trim();
			int indexTwo = first.indexOf("+");
			String visit_time = first.substring(indexOne + 1, indexTwo).trim();
			String[] arrayTwo = array[2].trim().split(" ");
			String status = arrayTwo[0].trim();
			String body_bytes_sent = arrayTwo[1].trim();
			String referer = array[3].trim();
			String areaid = EMPTY_CHAR;
			String http_user_agent = array[5].trim();
			char a = array[5].trim().charAt(0);
			if (Character.isDigit(a)) {
				areaid = String.valueOf(a);
				http_user_agent = http_user_agent.substring(1);
			}
			String forwarded = array[6].trim();
			String uuid = array[7].trim();
			String[] arrayThree = array[8].trim().split(" ");
			String request_time = arrayThree[0].trim();
			String response_time = arrayThree[1].trim();
			String mgjfrom = array[9].trim();
			String mgjuid = EMPTY_CHAR;
			int index = request.indexOf("_did=");
			int uid = request.indexOf("_uid=");
			if (index >= 0 && uid >= 0) {
				String s = request.substring(uid+5);
				index = s.indexOf("&");
				if (index >= 0) {
					mgjuid = s.substring(0, index);
				} else {
					mgjuid = s;
				}
			} else {
				mgjuid = array[11].trim();
			}
			String hostname = array[13].trim();
			String f_param = "";
			int indexFour = request.indexOf("f=");
			if (indexFour >= 0) {
				String s = request.substring(indexFour + 2);
				index = s.indexOf("&");
				indexTwo = s.indexOf("?");
				if (index >= 0) {
					int i = index;
					if (indexTwo >=0 && indexTwo < index) {
						i = indexTwo;
					}
					f_param = s.substring(0, i);
				} else if (indexTwo >= 0) {
					f_param = s.substring(0, indexTwo);
				} else {
					f_param = s;
				}
			}
			String request_type = array[1].split(" ")[0];
			String http_type = array[1].split(" ")[2];
			
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat f = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",
					Locale.ENGLISH);
			Date date = f.parse(visit_time);
			String visit_date = formatter.format(date);
			visit_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
			long time = date.getTime();
			long id = time + Math.round(Math.random() * 1000);
			String userid = parseUserid(mgjuid);
			request = UrlDecode(request);
			referer = UrlDecode(referer);
			mgjfrom = UrlDecode(mgjfrom);
			sb.append(id).append(split).append(ip).append(split)
					.append(remote_user).append(split).append(visit_time).append(split)
					.append(request).append(split).append(status).append(split)
					.append(body_bytes_sent).append(split).append(referer).append(split)
					.append(areaid).append(split).append(http_user_agent).append(split)
					.append(forwarded).append(split).append(uuid).append(split)
					.append(request_time).append(split).append(response_time).append(split)
					.append(mgjfrom).append(split).append(userid).append(split)
					.append(hostname).append(split).append(f_param).append(split)
					.append(request_type).append(split).append(http_type);
			returnArray[0] = sb.toString();
			returnArray[1] = visit_date;
		} catch (Exception e) {
			e.printStackTrace();
			returnArray[0] = EMPTY_CHAR;
			returnArray[1] = EMPTY_CHAR;
		}
		return returnArray;
	}

	private String parseUserid(String uid) {
		if (StringUtils.isEmpty(uid)) {
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
			url = URLDecoder.decode(URLDecoder.decode(src_url.replace("+", "%2b"), "UTF-8")
							.replace("+", "%2b"), "UTF-8");
		} catch (Exception e) {
			URLDecoder.decode("", "UTF-8");
			url = src_url.replace("%3A", ":").replace("%2F", "/");
		}
		return url;
	}
	
	public static void main(String args[]) throws Exception {
		NginxLogMapper alm = new NginxLogMapper();
		//System.out.println(alm.parseUserid("11cc3e2"));
		// String str =
		// "61.158.152.145 - [06/Jun/2013:15:53:55 +0800] \"GET /mogu.js?sfrom=www.google.com.hk&method=GET&time=1370504541&uuid=df158bc0-c0cf-22dc-12d1-43edc60a1b64&lady=&areaid=2&hahapoint=1344331211&refer=%2Fapi_xmgj_v310_book%2Fshopping%3F%26title%3D%E9%80%9B%E8%A1%97%E5%95%A6%26q%3D%E9%80%9B%E8%A1%97%E5%95%A6%26sort%3Dhot%26fcid%3D%26mbook%3DeyJxIjoiXCJcdTkwMWJcdTg4NTdcdTU1NjZcIiIsInFfbmF0dXJhbCI6IiIsInNvcnQiOiJob3Q3ZGF5IiwiY2Jvb2siOjEsImFjdGlvbiI6InNob3BwaW5nIiwicGFnZSI6MzYsInR5cGUiOiJhbGwiLCJjZ29vZHMiOjEsInRpbWVfZmFjdG9yIjoiMTVfOSIsImZjaWQiOiIiLCJwZXJwYWdlIjoyMH0%3D%26_source%3DXWAPV310%26_swidth%3D640%26t%3D1370505235%26callback%3D%3F&rerefer=http%3A%2F%2Fm.mogujie.com%2Fx%2Fwap%2Fwall%3Fparam%3Dmgj%253A%252F%252Fwall%252Fbook%252Fshopping%253F%2526title%253D%25E9%2580%259B%25E8%25A1%2597%25E5%2595%25A6%2526q%253D%25E9%2580%259B%25E8%25A1%2597%25E5%2595%25A6%2526sort%253Dhot%2526fcid%253D&anchor=&container=browser&callback=logCallBack&_=1370505235315&user_item_style=23123sdfs HTTP/1.1\" 200 \"http://m.mogujie.com/x/wap/wall?param=mgj%3A%2F%2Fwall%2Fbook%2Fshopping%3F%26title%3D%E9%80%9B%E8%A1%97%E5%95%A6%26q%3D%E9%80%9B%E8%A1%97%E5%95%A6%26sort%3Dhot%26fcid%3D\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 6_0_1 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A523 Safari/8536.25\"";
		String str = "182.243.67.91 - - [11/Jul/2013:15:43:24 +0800] \"GET / HTTP/1.1\" 200 34020 \"http://www.baidu.com/s?wd=%E8%98%91%E8%8F%87%E8%A1%97%E9%A6%96%E9%A1%B5&rsv_bp=0&ch=&tn=73072008_10_pg&bar=&rsv_spt=3&ie=utf-8&rsv_sug3=2&rsv_sug=0&rsv_sug1=2&rsv_sug4=125&oq=%E8%98%91%E8%8F%87%E8%A1%97&rsp=0&f=3&rsv_sug2=1&rsv_sug5=0&inputT=9500\" \"5Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; Microsoft Windows Media Center PC 6.0; Media Center PC 6.0)\" 182.243.67.91 \"\" 0.021 0.021 \"\" \"\" \"juanniu80\"";
		String[] array = alm.parseUrl(str);
		//System.out.println(array[0]);
		String[] ss = array[0].split(COLUMN_SPLIT);
		for (String s : ss) {
			System.out.println(s);
		}
		System.out.println(array[1]);
//		try {
//			String dd = alm.parseUserid("3lgc;");
//		} catch (Exception e) {
//			e.printStackTrace();
//			System.out.println("=========");
//		}
	}

}
