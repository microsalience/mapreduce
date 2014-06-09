package com.mogujie.app.mr.cnjie;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class NginxAccessCNjieJudge {
    private static final String HDFS_URI = "hdfs://qihe2061:8020";

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            @SuppressWarnings("resource")
            DistributedFileSystem dfs = new DistributedFileSystem();
            dfs.initialize(new URI(HDFS_URI), conf);
            // 正�?��????��????????�?
            List<NginxAccessCNjieLogFile> wFiles = new ArrayList<NginxAccessCNjieLogFile>();
            // ??��??已�?????�???????�?
            List<NginxAccessCNjieLogFile> cFiles = new ArrayList<NginxAccessCNjieLogFile>();
            for (int i = 0; i < args.length; i++) {
                try {
                    if (dfs.getClient()
                            .getNamenode()
                            .getBlockLocations(
                                    args[i],
                                    0,
                                    dfs.getFileStatus(new Path(args[i]))
                                            .getLen()).isUnderConstruction()) {
                        wFiles.add(new NginxAccessCNjieLogFile(args[i], "W"));
                    } else {
                        cFiles.add(new NginxAccessCNjieLogFile(args[i], "Y"));
                    }
                } catch (Exception e) {
                    wFiles.add(new NginxAccessCNjieLogFile(args[i], "F"));
                }
            }

            for (NginxAccessCNjieLogFile file : cFiles) {
                System.out.println(file.getFilePath() + " " + file.getStatus());
            }
            for (NginxAccessCNjieLogFile file : wFiles) {
                System.out.println(file.getFilePath() + " " + file.getStatus());
            }
        } catch (Exception e) {
            System.out.println("Error!!!!!!!!!!!!!!!!!!!!!");
            e.printStackTrace();
        }
    }
}
