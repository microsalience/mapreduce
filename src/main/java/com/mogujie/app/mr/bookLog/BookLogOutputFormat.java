/*
 * 蘑菇街 Inc.
 * Copyright (c) 2010-2014 All Rights Reserved.
 *
 * Author     :xingtian
 * Version    :1.0
 * Create Date:2014年5月23日
 */
package com.mogujie.app.mr.bookLog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.mogujie.app.mr.RCFileOutputFormat;

/**
 * @author xingtian
 * @version $Id: BookLogOutputFormat.java,v 0.1 2014年5月23日 下午3:31:29 xingtian
 *          Exp $
 */
public class BookLogOutputFormat<K extends WritableComparable<?>, V extends Writable>
		extends RCFileOutputFormat<K, V> {
	protected String generateFileNameForKeyValue(K key, V value,
			Configuration conf) {
		String dir = conf.get("mapred.output.dir");
		int index = dir.lastIndexOf("/");
		String name = dir.substring(index, dir.length());
		String filename = dir.substring(0, index + 1) + "visit_date="
				+ key.toString() + "/" + name;
		System.out.println(filename);
		return filename;
	}
}
