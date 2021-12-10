package com.subhadip.datory.preprocessing.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class HadoopUtil {
   private static final Logger LOG = LoggerFactory.getLogger(HadoopUtil.class);

   public static void addJarsToDistributedCache(Job job,
                                                String hdfsJarDirectory) 
      throws IOException {
      if (job == null) {
         return;
      }
      addJarsToDistributedCache(job.getConfiguration(), hdfsJarDirectory);
   }


   public static void addJarsToDistributedCache(Configuration conf,
                                                String hdfsJarDirectory) 
      throws IOException {
      if (conf == null) {
         return;
      }
      FileSystem fs = FileSystem.get(conf);
      List<FileStatus> jars = getDirectoryListing(hdfsJarDirectory, fs);
      for (FileStatus jar : jars) {
         Path jarPath = jar.getPath();
         DistributedCache.addFileToClassPath(jarPath, conf, fs);
      }
   }

   

    public static List<FileStatus> getDirectoryListing(String directory,
                                                       FileSystem fs)
       throws IOException {
       Path dir = new Path(directory);
       FileStatus[] fstatus = fs.listStatus(dir);
       return Arrays.asList(fstatus);
    }
    
    public static List<String> listDirectoryAsListOfString(String directory, 
                                                           FileSystem fs)
       throws IOException {
       Path path = new Path(directory);
       FileStatus fstatus[] = fs.listStatus(path);
       List<String> listing = new ArrayList<String>();
       for (FileStatus f: fstatus) {
           listing.add(f.getPath().toUri().getPath());
       }
       return listing;
    }

   public static boolean pathExists(Path path, FileSystem fs)  {
      if (path == null) {
         return false;
      }
      
      try {
         return fs.exists(path);
      }
      catch(Exception e) {
          return false;
      }
   }

   /**
    * Utility to merge part-xxxxx files to one file with name
    * @param hdfsConfig Configuration HDFS
    * @param srcDir     Source Directory Location of the part-xxxxx
    * @param destFilePath Destination Filepath with Filename of merged files
    * @param deleteSrc   Delete Source files?
    * @throws IOException
    * @throws URISyntaxException
    */
   public void mergeFiles(Configuration hdfsConfig, String srcDir, String destFilePath, boolean deleteSrc) throws URISyntaxException, IOException {
      LOG.info("MergeFiles = { srcDir: " + srcDir + ", destFilePath: " + destFilePath + ", deleteSrc: " + deleteSrc + "HDFS Config: " + hdfsConfig.toString() + "}");
      FileSystem hdfs = FileSystem.get(new URI(srcDir), hdfsConfig);
      FileUtil.copyMerge(hdfs, new Path(srcDir), hdfs, new Path(destFilePath),
                   deleteSrc, hdfsConfig, null);


   }

   public void mergeFiles(Configuration hdfsConfig, String srcDir, String destFilePath)
           throws IOException, URISyntaxException {
      mergeFiles(hdfsConfig, srcDir, destFilePath, false);
   }
   
}
