package com.mogujie.app.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 *
 * @author beifeng
 * @Date 2013-8-19 ä¸????5:38:15
 *
 */
public class VVLogNameMultipleTextOutputFormat extends MultipleOutputFormat<Text, Text> { 
    
	
    @Override 
    protected String generateFileNameForKeyValue(Text key, Text value, Configuration conf) {  
        String dir = conf.get("mapred.output.dir");
        int index = dir.lastIndexOf("/");
        String name = dir.substring(index, dir.length());
        String filename = dir.substring(0, index+1) + key.toString() + "/" + name; 
        return filename; 
    } 


} 
