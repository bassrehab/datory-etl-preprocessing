package com.subhadip.datory.preprocessing.core.stages;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.subhadipmitra.datory.preprocessing.common.exceptions.ApplicationException;
import com.subhadip.datory.preprocessing.model.PayloadModel;
import com.subhadip.datory.preprocessing.pipeline.StageInterface;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;

import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants.*;
import static com.subhadip.datory.preprocessing.core.shared.Parameters.*;
import static com.subhadip.datory.preprocessing.core.shared.Paths.*;

public class StageApplicationFinalize implements StageInterface<PayloadModel, PayloadModel>, Serializable {
    private static final long serialVersionUID = 6348559044756061219L;
    private static final Logger LOG = LoggerFactory.getLogger(StageApplicationInitialize.class);

    private final String STAGE_NAME = this.getClass().getCanonicalName();
    private PayloadModel payload;

    @Override
    public PayloadModel run(PayloadModel payload) {
        LOG.info("Starting State 4: Finalizing");
        this.payload = payload;

        SparkSession spark = SparkSession.builder().getOrCreate();
        Configuration hdfsConfig = new Configuration(spark.sparkContext().hadoopConfiguration());

        try{
            payload.getStatusModel().addToList(payload.getStatusModel().getStageLogs(), STAGE_NAME, STAGE_SUCCEEDED);
            payload.getStatusModel().setStatus(STAGE_SUCCEEDED); // mark processing as success
            payload.getStatusModel().setIsFinalStatus(_YES_); // mark final status

            // close any open database conn.
            payload.getParamsModel().getMetaDbConn().closeDatabaseConnection();

            flushLogs(hdfsConfig);
            if(isHousekeepingEnabled)
                doHousekeeping(hdfsConfig);

        }
        catch (ApplicationException e) {
            LOG.error(Throwables.getStackTraceAsString(e));
            payload.getStatusModel().setStatus(STAGE_FAILED);
            payload.getStatusModel().setIsFinalStatus(_YES_);
            payload.getStatusModel().addToList(payload.getStatusModel().getStageLogs(), STAGE_NAME, Throwables.getStackTraceAsString(e));
        }
        finally {
            LOG.info("Stopping spark Session..");
        }

        return payload;
    }

    private void flushLogs(Configuration hdfsConfig) throws ApplicationException {
        LOG.info("Flushing Logs..");

        try{
            ObjectMapper Obj = new ObjectMapper();
            String jsonLog = Obj.writeValueAsString(payload.getStatusModel());

            LOG.info(String.format("Flushing StatusModel.obj as JSON: \n%s,\n\n hdfsLogPath: %s", jsonLog, logPathHDFS));

            // Flush JSON to HDFS
            OutputStream os = FileSystem.get(hdfsConfig).create(new Path(logPathHDFS));
            BufferedWriter br = new BufferedWriter( new OutputStreamWriter( os, StandardCharsets.UTF_8) );
            br.write(jsonLog);
            br.close();

            // Flush from HDFS to Local
            LOG.info(String.format("Copying Application Status Logs from HDFS:%s to Local:%s", logPathHDFS, logPathLocal));
            FileSystem.get(hdfsConfig).copyToLocalFile(false, new Path(logPathHDFS), new Path(logPathLocal));
            //FileSystem.get(hdfsConfig).close();
        }
        catch(IOException e){
            throw new ApplicationException(Throwables.getStackTraceAsString(e), e);
        }

    }

    private void doHousekeeping(Configuration hdfsConfig) throws ApplicationException {

        try {
            FileSystem.get(hdfsConfig).delete(new Path(hdfsWorkingDir), true);
        } catch (IOException e) {
            throw new ApplicationException(Throwables.getStackTraceAsString(e), e);
        }


    }

}
