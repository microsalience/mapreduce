/*
 * ??????�? Inc.
 * Copyright (c) 2010-2014 All Rights Reserved.
 *
 * Author     :xingtian
 * Version    :1.0
 * Create Date:2014�?01???20???
 */
package com.mogujie.app.mr;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

/**
 * ?????��?��?????件�?��????��??
 * 
 * @author xingtian
 * @version $Id: GeneralFileJudge.java,v 0.1 2014�?01???20??? �????2:20:08
 *          xingtian Exp $
 */
public class GeneralFileJudge {
    private static final String HDFS_URI = "hdfs://qihe2061:8020";

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            @SuppressWarnings("resource")
            DistributedFileSystem dfs = new DistributedFileSystem();
            dfs.initialize(new URI(HDFS_URI), conf);
            // 正�?��????��????????�?
            List<GeneralLogFile> wFiles = new ArrayList<GeneralLogFile>();
            // ??��??已�?????�???????�?
            List<GeneralLogFile> cFiles = new ArrayList<GeneralLogFile>();
            for (int i = 0; i < args.length; i++) {
                try {
                    if (dfs.getClient()
                            .getNamenode()
                            .getBlockLocations(
                                    args[i],
                                    0,
                                    dfs.getFileStatus(new Path(args[i]))
                                            .getLen()).isUnderConstruction()) {
                        wFiles.add(new GeneralLogFile(args[i], "W"));
                    } else {
                        cFiles.add(new GeneralLogFile(args[i], "Y"));
                    }
                } catch (Exception e) {
                    wFiles.add(new GeneralLogFile(args[i], "F"));
                }
            }

            for (GeneralLogFile file : cFiles) {
                System.out.println(file.getFilePath() + " " + file.getStatus());
            }
            for (GeneralLogFile file : wFiles) {
                System.out.println(file.getFilePath() + " " + file.getStatus());
            }
        } catch (Exception e) {
            System.out.println("Error!!!!!!!!!!!!!!!!!!!!!");
            e.printStackTrace();
        }
    }
}
