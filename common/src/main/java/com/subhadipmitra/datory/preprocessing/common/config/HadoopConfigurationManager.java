package com.subhadipmitra.datory.preprocessing.common.config;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * ConfigurationManager create a Hadoop Configuration object

 */
public class HadoopConfigurationManager {

    private static final Logger LOG = Logger.getLogger(HadoopConfigurationManager.class);

    static String HADOOP_HOME = System.getenv("HADOOP_HOME");
    static String HADOOP_CONF_DIR = System.getenv("HADOOP_CONF_DIR");

    static final String HADOOP_CONF_CORE_SITE = HadoopConfigurationManager.getHadoopConfDir() + "/core-site.xml";
    static final String HADOOP_CONF_HDFS_SITE = HadoopConfigurationManager.getHadoopConfDir() + "/hdfs-site.xml";
    static final String HADOOP_CONF_MAPRED_SITE = HadoopConfigurationManager.getHadoopConfDir() + "/mapred-site.xml";
    static final String HADOOP_CONF_YARN_SITE = HadoopConfigurationManager.getHadoopConfDir() + "/yarn-site.xml";


    public static void setHadoopHomeDir(String dir) {
        HADOOP_HOME = dir;
    }

    public static String getHadoopHomeDir() {
        return HADOOP_HOME;
    }


    public static void setHadoopConfDir(String dir) {
        HADOOP_CONF_DIR = dir;
    }

    public static String getHadoopConfDir() {
        return HADOOP_CONF_DIR;
    }

    public static Configuration createConfiguration() throws IOException {
        LOG.info("createConfiguration() started.");
        LOG.info("createConfiguration() HADOOP_HOME=" + HadoopConfigurationManager.getHadoopHomeDir());
        LOG.info("createConfiguration() HADOOP_CONF_DIR=" + HadoopConfigurationManager.getHadoopConfDir());

        Configuration config = new Configuration();

        config.addResource(new File(HADOOP_CONF_CORE_SITE).getAbsoluteFile().toURI().toURL());   // WORKED
        config.addResource(new File(HADOOP_CONF_HDFS_SITE).getAbsoluteFile().toURI().toURL());   // WORKED
        config.addResource(new File(HADOOP_CONF_MAPRED_SITE).getAbsoluteFile().toURI().toURL()); // WORKED
        config.addResource(new File(HADOOP_CONF_YARN_SITE).getAbsoluteFile().toURI().toURL());   // WORKED

        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()); // WORKED
        config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());         // WORKED
        config.set("hadoop.home.dir", HadoopConfigurationManager.getHadoopHomeDir());
        config.set("hadoop.conf.dir", HadoopConfigurationManager.getHadoopConfDir());
        config.set("yarn.conf.dir", HadoopConfigurationManager.getHadoopConfDir());
        //
        //config.reloadConfiguration();
        //
        LOG.info("createConfiguration(): Configuration created.");
        return config;
    }

}