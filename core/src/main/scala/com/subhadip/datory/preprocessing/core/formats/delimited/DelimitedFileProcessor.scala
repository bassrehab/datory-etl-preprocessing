package com.subhadipmitra.datory.preprocessing.core.formats.delimited

import java.io.IOException
import java.net.URISyntaxException

import org.apache.log4j.Logger
import ApplicationConstants.{_DATA_ROW_, _EMPTY_STRING_, _NULL_LITERAL_}
import DelimitedFileAttributes._
import Paths._
import HadoopUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Map

@SerialVersionUID(7046334157374201118L)
class DelimitedFileProcessor() extends Serializable  {

  object Wrapper extends Serializable {
    @transient lazy val LOG: Logger = Logger.getLogger(getClass.getName)
  }

  @throws(classOf[Exception])
  def execute(spark:SparkSession, hdfsConfig: Configuration, maxCols: Int,
              controlCharReplacementMap: Map[String, String], skipErrRecords: Boolean): Unit ={


    Wrapper.LOG.info("Starting Delimited File Processor...")
    val hdfs = FileSystem.get(hdfsConfig)

    // copyFromLocalToHDFS
    Wrapper.LOG.info("Copying Local File %s to HDFS %s".format(inputFilePathLocal, inputFilePathHdfs))

    hdfs.copyFromLocalFile(new Path(inputFilePathLocal), new Path(inputFilePathHdfs))



    val DATA_DECISION_BOUNDARY_COND = "%s%s%s".format("\n", delimitedRecordStart,
      "\u0007")

    Wrapper.LOG.info("DATA_DECISION_BOUNDARY_COND = " + DATA_DECISION_BOUNDARY_COND)

    val DATA_DECISION_BOUNDARY_COND_MARKER = "%s%s%s".format(_DATA_ROW_,
      delimitedRecordStart, "\u0007")
    Wrapper.LOG.info("DATA_DECISION_BOUNDARY_COND_MARKER = " + DATA_DECISION_BOUNDARY_COND_MARKER)

    val sc = spark.sparkContext
    val inputFileRDD = sc.wholeTextFiles(inputFilePathHdfs)

    val headerFooterRDD = inputFileRDD.map(x=>x._2.split("\n")).first()
    val header  = headerFooterRDD.head // header String
    val footer = headerFooterRDD.last // footer String

    Wrapper.LOG.info("Extracted Header: %s and Footer:%s".format(header, footer))

    // remove header footer and get rid of first \nD\u0007
    var dataRDD = inputFileRDD.flatMap(x => x._2
      .replaceFirst("\n", "")
      .replace(DATA_DECISION_BOUNDARY_COND, DATA_DECISION_BOUNDARY_COND_MARKER)
      .replace(header, "")
      .replace(footer, "")
      .replace("\r", " ")
      .replace("\n", " ")
      .split(_DATA_ROW_))


    /*import spark.implicits._
    dataRDD.toDS()

    val dataDS = dataRDD.toDS
    val newlineDS = dataDS.filter(row => row != "")
      .zipWithIndex().filter(_._2 > 0).keys*/

    Wrapper.LOG.info("Control Character Replacement Map:" + controlCharReplacementMap)
    // Do Control Character Removal
    for ((regex,replacement) <- controlCharReplacementMap)
      dataRDD.map(line => line.replaceAll(regex, replacement))


    Wrapper.LOG.info("Flag (-p) Skip Error Records in Pre Processing: " + skipErrRecords)

    if(skipErrRecords){
      Wrapper.LOG.info("Seggregating Error Records from Clean Records..")
      val (goodRowsRDD, badRowsRDD) = handleErrorRecords(spark, dataRDD, maxCols)

      Wrapper.LOG.info("Flushing Clean Records to Local File..")
      flushRDDToFile(goodRowsRDD, hdfsConfig, tempDirHdfsClean,
        tempFilePathHdfsCombineClean, outputFilePathLocalClean, isDeleteSrc = false)

      Wrapper.LOG.info("Flushing Error Records to Local File..")
      flushRDDToFile(badRowsRDD, hdfsConfig, tempDirHdfsBad, tempFilePathHdfsCombineBad,
        outputFilePathLocalBad, isDeleteSrc = false)
    }
    else{
      Wrapper.LOG.info("Flushing Records to Local File..")
      flushRDDToFile(dataRDD, hdfsConfig, tempDirHdfsClean,
        tempFilePathHdfsCombineClean, outputFilePathLocalClean, isDeleteSrc = false)

      // create an empty error file (because thats what the EDF framework expects, even if there are 0 error records)
      createEmptyErrorFile(hdfsConfig)
    }

    Wrapper.LOG.info("Completed Pre-processing..")
  }

  @throws(classOf[Exception])
  def combineHeaderFooter(sc:SparkContext, header:String, dataRDD:RDD[String], footer:String): RDD[String] ={
    val headerRDD: RDD[String] = sc.parallelize(List(header))
    val footerRDD: RDD[String] = sc.parallelize(List(footer))

    Wrapper.LOG.info("Unionising RDD={ headerRDD: %s, dataRDD: %s, footerRDD: %s}"
      .format(getRDDDetails(headerRDD), getRDDDetails(dataRDD), getRDDDetails(dataRDD)))

    sc.union(headerRDD, dataRDD, footerRDD)
  }

  @throws(classOf[Exception])
  def handleErrorRecords(spark:SparkSession, dataRDD:RDD[String], expectedCols:Int): (RDD[String], RDD[String]) ={
    import spark.implicits._

    val dataDF = dataRDD
      .map(_.split("\u0007"))
      .toDF("arr")
      .select((0 until expectedCols).map(i => $"arr"(i).as(s"col_$i")): _*)


    val badRowsRDD = dataDF
      .filter(row => row.anyNull)
      .rdd // convert to RDD for flushing
      .map(x=> x.mkString("\u0007"))
      .map(x=> x.replace("%s%s".format("\u0007", _NULL_LITERAL_), _EMPTY_STRING_))// replace the nulls that we got from DF

    val goodRowsRDD = dataDF
      .filter(row => !row.anyNull) // can also use exceptAll, but stages will be recomputed
      .rdd
      .map(x=> x.mkString("\u0007"))

    (goodRowsRDD,badRowsRDD)
  }

  @throws(classOf[Exception])
  @throws(classOf[IOException])
  @throws(classOf[URISyntaxException])
  def flushRDDToFile(rddToFlush: RDD[String], hdfsConfig: Configuration, srcDirPartFiles: String,
                     destHDFSFile: String, destLocalFile: String, isDeleteSrc: Boolean): Unit ={
    Wrapper.LOG.info("Flushing RDD={id: %d, name=%s, HDFS_Dir: %s}".format(rddToFlush.id, rddToFlush.name, srcDirPartFiles) )
    rddToFlush.saveAsTextFile(srcDirPartFiles)

    // Merge Rdd as one
    Wrapper.LOG.info("Merging part-xxxxx files: %s".format(destHDFSFile))
    new HadoopUtil().mergeFiles(hdfsConfig, srcDirPartFiles, destHDFSFile, isDeleteSrc)

    // copy from HDFS to Local
    Wrapper.LOG.info("Copying HDFS File: %s, to Local File: %s".format(destHDFSFile, destLocalFile) )
    FileSystem.get(hdfsConfig).copyToLocalFile(false, new Path(destHDFSFile), new Path(destLocalFile))
  }

  def getRDDDetails(rdd: RDD[String]): String ={
    "{ id: " + rdd.id + ", name: " + rdd.id + ", storageLevel: " +rdd.getStorageLevel + "}"
  }

  @throws(classOf[Exception])
  @throws(classOf[IOException])
  @throws(classOf[URISyntaxException])
  def createEmptyErrorFile(hdfsConfig: Configuration): Unit ={
    Wrapper.LOG.info("Creating Dummy Empty Error File on HDFS : %s".format(tempFilePathHdfsCombineBad))
    FileSystem.get(hdfsConfig).create(new Path(tempFilePathHdfsCombineBad)) // file created

    Wrapper.LOG.info("Copying Dummy Error File file from HDFS: %s to Local:%s".format(tempFilePathHdfsCombineBad, outputFilePathLocalBad) )
    FileSystem.get(hdfsConfig).copyToLocalFile(false, new Path(tempFilePathHdfsCombineBad), new Path(outputFilePathLocalBad))
  }

}



