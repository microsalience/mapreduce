package com.mogujie.app.mr.itemsaction;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URLDecoder;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

public class ItemsActionMapper extends
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
		String[] whole = str.split(TAB);
		if (whole.length != 3) {
			return null;
		}
		// ??��??记�?? ??��?? 2012-01-01 00:00:00
		String rub_visit_time = "2012-01-01 00:00:00";
		String visit_time = "";
		String[] header = whole[0].split(" ");
		Boolean record_status = true;
		if (header.length < 2) {
			visit_time = rub_visit_time;
			record_status = false;
		} else {
			visit_time = header[0] + " " + header[1];
		}
		if (visit_time.length() != 19) {
			visit_time = rub_visit_time;
			record_status = false;
		}
		BigInteger id = new BigInteger(header[2]);
		String visit_date = visit_time.split(" ")[0];
		String act = whole[1];
		String throughput = null;
		BigInteger logId = new BigInteger("0");
		BigInteger time = new BigInteger("0");
		String action = null;
		String uuid = null;
		String uid = null;
		String ip = null;
		String url = null;
		String referer = null;
		String currentId = null;
		String preId = null;
		String metadata = null;
		String web = null;
		String refChannel = "";
		try {
			JSONObject jsonobject = new JSONObject(whole[2]);
			throughput = jsonobject.getString("throughput");
			logId = new BigInteger(jsonobject.getString("logId"));
			time = new BigInteger(jsonobject.getString("time"));
			action = jsonobject.getString("action");
			uuid = jsonobject.getString("uuid");
			uid = jsonobject.getString("uid");
			refChannel = jsonobject.getString("refChannel");
			ip = jsonobject.getString("ip");
			url = jsonobject.getString("url");
			referer = urlDecode(jsonobject.getString("referer"));
			currentId = jsonobject.has("currentId") ? jsonobject
					.getString("currentId") : null;
			preId = jsonobject.has("preId") ? jsonobject.getString("preId")
					: null;
			metadata = jsonobject.getString("metadata");
			web = jsonobject.getString("web");
		} catch (JSONException e) {
			e.printStackTrace();
		}
		if (!record_status) {
			visit_time = str;
			System.out.println(str);
		}
		sb.append(id).append(split).append(act).append(split)
				.append(visit_time).append(split).append(throughput)
				.append(split).append(logId).append(split).append(time)
				.append(split).append(action).append(split).append(uuid)
				.append(split).append(uid).append(split).append(ip)
				.append(split).append(url).append(split).append(referer)
				.append(split).append(currentId).append(split).append(preId)
				.append(split).append(metadata).append(split).append(web)
				.append(split).append(refChannel);
		String[] result = new String[2];
		result[0] = sb.toString();
		result[1] = visit_date;
		return result;
	}

	private String urlDecode(String s) {
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
			url = src_url.replace("%3A", ":").replace("%2F", "/");
		}
		return url;
	}

	public static void main(String args[]) throws Exception {
		ItemsActionMapper alm = new ItemsActionMapper();
		String str = "2013-12-10 00:00:00 103355375684804608	book	{\"throughput\":\"b2\",\"logId\":103355375684804608,\"time\":1386604800,\"action\":\"book\",\"uuid\":\"4e63bfb0-8782-658f-8040-97f52bae7df9\",\"uid\":0,\"refChannel\":\"user.qzone.qq.com|201405231045YYGJtxkj\",\"ip\":\"221.15.125.36\",\"url\":\"http://www.mogujie.com/book/ajax\",\"referer\":\"http%3A%2F%2Fwww.mogujie.com%2Fbook%2Fshopping%2F%2F6%2Fpop%2Fall%2F%3Fcolor%3D%26minPrice%3D%26maxPrice%3D%26fc%3D%26fc_v%3D%26f%3D%26youdian%3D1%26atc%3D52a5e8dc72e011.91684433\",\"currentId\":\"book_s9of81v3hts\",\"preId\":null,\"metadata\":[{\"ad_info\":{\"cur_id\":\"103355375655444480\",\"cis\":\"7\",\"ces\":\"0\"},\"twitterId\":586970204},{\"ad_info\":{\"cur_id\":\"103355375655444481\",\"cis\":\"7\",\"ces\":\"0\"},\"twitterId\":586852850},{\"ad_info\":{\"cur_id\":\"103355375655444482\",\"cis\":\"7\",\"ces\":\"0\"},\"twitterId\":586961916},{\"ad_info\":{\"cur_id\":\"103355375655444483\",\"cis\":\"7\",\"ces\":\"0\"},\"twitterId\":586966280},{\"ad_info\":{\"cur_id\":\"103355375655444484\",\"cis\":\"7\",\"ces\":\"0\"},\"twitterId\":586823428},{\"ad_info\":{\"cur_id\":\"103355375655444485\",\"cis\":\"7\",\"ces\":\"0\"},\"twitterId\":583827296},{\"ad_info\":{\"cur_id\":\"103355375655444486\",\"cis\":\"7\",\"ces\":\"0\"},\"twitterId\":582715297},{\"ad_info\":{\"cur_id\":\"103355375655444487\",\"cis\":\"7\",\"ces\":\"0\"},\"twitterId\":586972368},{\"ad_info\":{\"cur_id\":\"103355375659638784\",\"cis\":\"7\",\"ces\":\"0\"},\"twitterId\":586812599},{\"ad_info\":{\"cur_id\":\"103355375659638785\",\"cis\":\"7\",\"ces\":\"0\"},\"twitterId\":583606084},{\"ad_info\":{\"cur_id\":\"103355375659638786\",\"cis\":\"7\",\"ces\":\"0\"},\"twitterId\":586878721},{\"ad_info\":{\"cur_id\":\"103355375659638787\",\"cis\":\"7\",\"ces\":\"0\"},\"twitterId\":586816600}],\"web\":0}";
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
