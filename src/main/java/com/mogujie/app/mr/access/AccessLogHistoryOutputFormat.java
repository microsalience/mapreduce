package com.mogujie.app.mr.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.mogujie.app.mr.RCFileOutputFormat;


/**
 *
 * @author beifeng
 * @Date 2013-8-21 ä¸????1:15:40
 *
 */
public class AccessLogHistoryOutputFormat<K extends WritableComparable<?>, V extends Writable> extends RCFileOutputFormat<K, V> {

	protected String generateFileNameForKeyValue(K key, V value,
			Configuration conf) {
		String dir = conf.get("mapred.output.dir");
        int index = dir.lastIndexOf("/");
        String name = dir.substring(index, dir.length());
        String[] array = key.toString().split(AccessLogMapper.KEY_SPLIT);
        String filename = dir.substring(0, index+1) + "visit_date=" + array[0] + "/" + name; 
        System.out.println(filename);
        return filename;
	}	
	
}
