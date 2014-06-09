package com.mogujie.app.mr;

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
 * @Date 2013-5-2 �????7:23:35
 * 
 */
public class AccessLogMapper extends
        Mapper<Object, Text, Text, BytesRefArrayWritable> {

    public static final String COLUMN_SPLIT = "\001";
    public static final String EMPTY_CHAR = "";
    public static final String KEY_SPLIT = ",";

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

    @SuppressWarnings("deprecation")
    private String[] parseUrl(String str) {
        String[] returnArray = new String[2];
        try {
            StringBuilder sb = new StringBuilder(512);
            String split = COLUMN_SPLIT;
            String[] array = str.split("\"");
            String first = array[0];
            int indexThree = first.indexOf("-");
            String ip = first.substring(0, indexThree).trim();
            String request = array[1].split(" ")[1].trim();
            int index = request.indexOf("&uid=");
            String uid = EMPTY_CHAR;
            if (index >= 0) {
                String s = request.substring(index + 5);
                uid = cleanString(s);
            }
            String uuid = EMPTY_CHAR;
            index = request.indexOf("&uuid=");
            if (index >= 0) {
                String s = request.substring(index + 6);
                uuid = cleanString(s);
            }
            String url = array[3];
            String refer = EMPTY_CHAR;
            index = request.indexOf("&refer=");
            if (index >= 0) {
                String s = request.substring(index + 7);
                refer = cleanString(s);
            }
            String rerefer = EMPTY_CHAR;
            index = request.indexOf("&rerefer=");
            if (index >= 0) {
                String s = request.substring(index + 9);
                rerefer = cleanString(s);
            }
            int indexOne = first.indexOf("[");
            int indexTwo = first.indexOf("+");
            String server_time = first.substring(indexOne + 1, indexTwo).trim();
            /**
             * String client_time = EMPTY_CHAR; index =
             * request.indexOf("&time="); if (index >= 0) { String s =
             * request.substring(index + 6); client_time = cleanString(s); }
             * String method = EMPTY_CHAR; index = request.indexOf("&method=");
             * if (index >= 0) { String s = request.substring(index + 8); method
             * = cleanString(s); }
             */
            String status = array[2].trim();
            String client = array[5];
            String sfrom = EMPTY_CHAR;
            index = request.indexOf("?sfrom=");
            if (index >= 0) {
                String s = request.substring(index + 7);
                sfrom = cleanString(s);
            }
            String lady = EMPTY_CHAR;
            index = request.indexOf("&areaid=");
            if (index >= 0) {
                String s = request.substring(index + 8);
                lady = cleanString(s);
            }
            String protocol = array[1].split(" ")[2];
            String req = array[3];
            String f_argv = EMPTY_CHAR;
            index = request.indexOf("?f=");
            if (index < 0) {
                index = request.indexOf("&f=");
            }
            if (index >= 0) {
                String s = req.substring(index + 3);
                index = s.indexOf("&");
                indexTwo = s.indexOf("?");
                if (index >= 0) {
                    int i = index;
                    if (indexTwo >= 0 && indexTwo < index) {
                        i = indexTwo;
                    }
                    f_argv = s.substring(0, i);
                } else if (indexTwo >= 0) {
                    f_argv = s.substring(0, indexTwo);
                } else {
                    f_argv = s;
                }
            }
            String anchor = EMPTY_CHAR;
            index = request.indexOf("&anchor=");
            if (index >= 0) {
                String s = request.substring(index + 8);
                anchor = cleanString(s);
            }
            String callback = EMPTY_CHAR;
            index = request.indexOf("&callback=");
            if (index >= 0) {
                String s = request.substring(index + 10);
                callback = cleanString(s);
            }
            String hahapoint = EMPTY_CHAR;
            index = request.indexOf("&hahapoint=");
            if (index >= 0) {
                String s = request.substring(index + 11);
                hahapoint = cleanString(s);
            }
            String container = EMPTY_CHAR;
            index = request.indexOf("&container=");
            if (index >= 0) {
                String s = request.substring(index + 11);
                container = cleanString(s);
            }
            String extra_param = " ";
            index = request.indexOf("&areaid=");
            if (index >= 0) {
                String s = request.substring(index + 8);
                extra_param = cleanString(s);
            }
            index = request.indexOf("&user_item_style=");
            String userItem = " ";
            if (index >= 0) {
                String s = request.substring(index + 17);
                userItem = cleanString(s);
            }
            extra_param += "&" + userItem;
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            SimpleDateFormat f = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",
                    Locale.ENGLISH);
            Date date = f.parse(server_time);
            String visit_date = formatter.format(date);
            String visit_time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                    .format(date);
            long time = date.getTime();
            long id = time + Math.round(Math.random() * 1000);
            String site = "others";
            if (request.contains("mogujie.com")) {
                site = "mogujie";
            } else if (request.contains("mogujia.com")) {
                site = "mogujia";
            }
            String userid = parseUserid(uid);
            url = UrlDecode(url);
            rerefer = UrlDecode(rerefer);
            String rerefer_www = getHost(rerefer);
            sfrom = UrlDecode(sfrom);
            String sfrom_www = getHost(sfrom);
            if (refer.contains("ajax") || refer.contains("itunes.apple.com")) {
                refer = UrlDecode(refer);
            } else if (!refer.contains("xwap_close")) {
                refer = EMPTY_CHAR;
            }
            sb.append(id).append(split).append(site).append(split)
                    .append(userid).append(split).append(uuid).append(split)
                    .append(url).append(split).append(rerefer).append(split)
                    .append(rerefer_www).append(split).append(ip).append(split)
                    .append(ipToNum(ip)).append(split).append(0).append(split)
                    .append(visit_time).append(split).append(status)
                    .append(split).append(client).append(split).append(sfrom)
                    .append(split).append(sfrom_www).append(split).append(lady)
                    .append(split).append(protocol).append(split)
                    .append(f_argv).append(split).append(anchor).append(split)
                    .append(callback).append(split).append(hahapoint)
                    .append(split).append(refer).append(split)
                    .append(container).append(split).append(extra_param)
                    .append(split);
            returnArray[0] = sb.toString();
            returnArray[1] = visit_date + KEY_SPLIT + date.getHours();
        } catch (Exception e) {
            e.printStackTrace();
            returnArray[0] = str;
            returnArray[1] = "2000-00-00";
        }
        return returnArray;
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

    private String parseUserid(String uid) {
        if (StringUtils.isEmpty(uid)) {
            return EMPTY_CHAR;
        }
        String strUserid = uid.substring(1);
        long userid = 0;
        // �?36�???��??�?�?�?str??????�?�?BigInteger
        BigInteger big = new BigInteger(strUserid, 36);
        // �?�?�?36�???�转???�?10�???�表示�??�?�?�?
        String strUserid10 = big.toString(10);
        // 计�??userid
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

    private String getHost(String s) {
        if (StringUtils.isEmpty(s)) {
            return EMPTY_CHAR;
        }
        int index = s.indexOf("://");
        if (index > 0) {
            String ss = s.substring(index + 3, s.length());
            index = ss.indexOf("/");
            if (index > 0) {
                ss = ss.substring(0, index);
            }
            return ss;
        } else {
            return EMPTY_CHAR;
        }
    }

    private String ipToNum(String ip) {
        String returnValue = EMPTY_CHAR;
        try {
            long num = 0;
            String[] sections = ip.split("\\.");
            int i = 3;
            for (String str : sections) {
                num += (Long.parseLong(str) << (i * 8));
                i--;
            }
            returnValue = String.valueOf(num);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return returnValue;
    }

    public static void main(String args[]) throws Exception {
        AccessLogMapper alm = new AccessLogMapper();
        System.out.println(alm.parseUserid("11cc3e2"));
        // String str =
        // "61.158.152.145 - [06/Jun/2013:15:53:55 +0800] \"GET /mogu.js?sfrom=www.google.com.hk&method=GET&time=1370504541&uuid=df158bc0-c0cf-22dc-12d1-43edc60a1b64&lady=&areaid=2&hahapoint=1344331211&refer=%2Fapi_xmgj_v310_book%2Fshopping%3F%26title%3D%E9%80%9B%E8%A1%97%E5%95%A6%26q%3D%E9%80%9B%E8%A1%97%E5%95%A6%26sort%3Dhot%26fcid%3D%26mbook%3DeyJxIjoiXCJcdTkwMWJcdTg4NTdcdTU1NjZcIiIsInFfbmF0dXJhbCI6IiIsInNvcnQiOiJob3Q3ZGF5IiwiY2Jvb2siOjEsImFjdGlvbiI6InNob3BwaW5nIiwicGFnZSI6MzYsInR5cGUiOiJhbGwiLCJjZ29vZHMiOjEsInRpbWVfZmFjdG9yIjoiMTVfOSIsImZjaWQiOiIiLCJwZXJwYWdlIjoyMH0%3D%26_source%3DXWAPV310%26_swidth%3D640%26t%3D1370505235%26callback%3D%3F&rerefer=http%3A%2F%2Fm.mogujie.com%2Fx%2Fwap%2Fwall%3Fparam%3Dmgj%253A%252F%252Fwall%252Fbook%252Fshopping%253F%2526title%253D%25E9%2580%259B%25E8%25A1%2597%25E5%2595%25A6%2526q%253D%25E9%2580%259B%25E8%25A1%2597%25E5%2595%25A6%2526sort%253Dhot%2526fcid%253D&anchor=&container=browser&callback=logCallBack&_=1370505235315&user_item_style=23123sdfs HTTP/1.1\" 200 \"http://m.mogujie.com/x/wap/wall?param=mgj%3A%2F%2Fwall%2Fbook%2Fshopping%3F%26title%3D%E9%80%9B%E8%A1%97%E5%95%A6%26q%3D%E9%80%9B%E8%A1%97%E5%95%A6%26sort%3Dhot%26fcid%3D\" \"Mozilla/5.0 (iPhone; CPU iPhone OS 6_0_1 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A523 Safari/8536.25\"";
        String str = "223.199.206.158 - [20/Aug/2013:09:55:46 +0800] \"GET /mogu.js?sfrom=so.360.cn&method=GET&time=1376963742&uuid=67ac14af-241e-4bc8-1c53-b109611b39ec&lady=&areaid=5&hahapoint=3f565e1a84effd52e18701c96122d70b&refer=%2Fbook%2Fsearch%2F%25E6%2597%2585%25E8%25A1%258C%25E7%25AE%25B1%2520%25E4%25B8%2587%25E5%2590%2591%25E8%25BD%25AE%2F1%2Fpop%2Fbao%2F%3Fuser%3D1&rerefer=http%3A%2F%2Fwww.mogujie.com%2Fbook%2Fclothing%2F&anchor= HTTP/1.1\" 200 \"http://www.mogujie.com/book/search/%E6%97%85%E8%A1%8C%E7%AE%B1%20%E4%B8%87%E5%90%91%E8%BD%AE/1/pop/bao/?user=1\" \"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; QQDownload 718; InfoPath.2; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)\"";
        System.out.println(alm.UrlDecode(str));
        String[] array = alm.parseUrl(str);
        System.out.println(array[0]);
        String[] ss = array[0].split(COLUMN_SPLIT);
        for (String s : ss) {
            System.out.println(s);
        }
        System.out.println(array[1]);
    }

}
