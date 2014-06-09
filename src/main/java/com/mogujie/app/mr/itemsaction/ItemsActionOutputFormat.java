package com.mogujie.app.mr.itemsaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.mogujie.app.mr.RCFileOutputFormat;


public class ItemsActionOutputFormat<K extends WritableComparable<?>, V extends Writable> extends RCFileOutputFormat<K, V> {
	protected String generateFileNameForKeyValue(K key, V value,
			Configuration conf) {
		String dir = conf.get("mapred.output.dir");
        int index = dir.lastIndexOf("/");
        String name = dir.substring(index, dir.length());
        String filename = dir.substring(0, index+1) + "visit_date=" + key.toString() + "/" + name; 
        System.out.println(filename);
        return filename;
	}
}
