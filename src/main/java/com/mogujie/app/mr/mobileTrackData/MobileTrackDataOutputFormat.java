/*
 * ??????è¡? Inc.
 * Copyright (c) 2010-2014 All Rights Reserved.
 *
 * Author     :xingtian
 * Version    :1.0
 * Create Date:2014å¹?3???3???
 */
package com.mogujie.app.mr.mobileTrackData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import com.mogujie.app.mr.RCFileOutputFormat;

/**
 * @author xingtian
 * @version $Id: MobileTrackDataOutputFormat.java,v 0.1 2014å¹?3???3??? ä¸????5:17:10
 *          xingtian Exp $
 */
public class MobileTrackDataOutputFormat<K extends WritableComparable<?>, V extends Writable>
		extends RCFileOutputFormat<K, V> {
	@Override
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
