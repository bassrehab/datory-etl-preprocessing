package com.subhadip.datory.preprocessing.core.formats.delimited;

import com.google.common.base.Throwables;
import com.subhadip.datory.preprocessing.core.shared.Parameters;
import com.subhadip.datory.preprocessing.core.shared.Paths;
import com.subhadipmitra.datory.preprocessing.common.exceptions.ApplicationException;
import com.subhadip.datory.preprocessing.model.PayloadModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;

import static com.subhadip.datory.preprocessing.core.formats.delimited.DelimitedFileAttributes.delimitedExpectedColsNum;

public class DelimitedFileDriver implements Serializable {

    private static final long serialVersionUID = -3365282072505255557L;
    private static final Logger LOG = LoggerFactory.getLogger(DelimitedFileDriver.class);

    public void execute (PayloadModel payload) throws ApplicationException {

        try {
            SparkSession spark = SparkSession.builder().getOrCreate();

            Configuration hdfsConfig = new Configuration(spark.sparkContext().hadoopConfiguration());
            FileSystem hdfs = FileSystem.get(hdfsConfig);

            // copyFromLocalToHDFS
            LOG.info(String.format("Copying File: %s to HDFS %s", Paths.inputFilePathLocal, Paths.inputFilePathHdfs));
            hdfs.copyFromLocalFile(new Path(Paths.inputFilePathLocal), new Path(Paths.inputFilePathHdfs));


            // Execute
            LOG.info(String.format("Beginning pre processing of Proc ID: %s, ProcInstanceId: %s, Skip Error Records is:  %s",
                    payload.getParamsModel().getProcId(), payload.getParamsModel().getProcInstanceId(), Parameters.isSkipErrRecords));

            // refer Scala Class
            new DelimitedFileProcessor().execute(spark,hdfsConfig, delimitedExpectedColsNum,
                    toScalaImmutableMap(payload.getSourceModel().getControlCharReplacementMap()), Parameters.isSkipErrRecords);

        }
        catch (Exception e) {
            LOG.error(Throwables.getStackTraceAsString(e));
            throw new ApplicationException(Throwables.getStackTraceAsString(e), e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <K, V> scala.collection.immutable.Map<K, V> toScalaImmutableMap(java.util.Map<K, V> javaMap) {
        final java.util.List<scala.Tuple2<K, V>> list = new java.util.ArrayList<>(javaMap.size());
        for (final java.util.Map.Entry<K, V> entry : javaMap.entrySet()) {
            list.add(scala.Tuple2.apply(entry.getKey(), entry.getValue()));
        }
        final scala.collection.Seq<Tuple2<K, V>> seq = scala.collection.JavaConverters.asScalaBufferConverter(list).asScala().toSeq();
        return (scala.collection.immutable.Map<K, V>) scala.collection.immutable.Map$.MODULE$.apply(seq);
    }



}
