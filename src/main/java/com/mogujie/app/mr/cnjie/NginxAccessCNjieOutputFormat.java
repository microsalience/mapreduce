/*
 * ??????è¡? Inc.
 * Copyright (c) 2010-2013 All Rights Reserved.
 *
 * Author     :yinxiu
 * Version    :1.0
 * Create Date:2013å¹?10???25???
 */
package com.mogujie.app.mr.cnjie;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.mogujie.app.mr.RCFileOutputFormat;

/**
 * @author yinxiu
 * @version $Id: NginxAccessCNjieOutputFormat.java,v 0.1 2013å¹?10???25??? ä¸????2:22:16
 *          yinxiu Exp $
 */
public class NginxAccessCNjieOutputFormat<K extends WritableComparable<?>, V extends Writable>
		extends RCFileOutputFormat<K, V> {
	@Override
	protected String generateFileNameForKeyValue(K key, V value,
			Configuration conf) {
		String dir = conf.get("mapred.output.dir");
		int index = dir.lastIndexOf("/");
		String name = dir.substring(index, dir.length());
		String[] array = key.toString().split(NginxAccessCNjieMapper.KEY_SPLIT);
		String filename = dir.substring(0, index + 1) + "visit_date="
				+ array[0] + "/visit_hour=" + array[1] + "/" + name;
		System.out.println(filename);
		return filename;
	}

}
