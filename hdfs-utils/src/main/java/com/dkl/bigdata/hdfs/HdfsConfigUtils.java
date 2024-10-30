package com.dkl.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class HdfsConfigUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsConfigUtils.class);

    /**
     * 初始化HDFS Configuration
     *
     * @return configuration
     */
    public static Configuration initConfiguration(String confPath) {
        Configuration configuration = new Configuration();
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.addResource(new Path(new File(confPath + File.separator + "core-site.xml").toURI()));
        configuration.addResource(new Path(new File(confPath + File.separator + "hdfs-site.xml").toURI()));
        configuration.addResource(new Path(new File(confPath + File.separator + "yarn-site.xml").toURI()));
        return configuration;
    }

}
