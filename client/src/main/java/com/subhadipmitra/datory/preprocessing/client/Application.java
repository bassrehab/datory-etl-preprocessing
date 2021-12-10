package com.subhadipmitra.datory.preprocessing.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.subhadipmitra.datory.hook.nterfaces.HookFrameworkInterface;
import com.subhadipmitra.datory.preprocessing.common.exceptions.ApplicationException;
import com.subhadip.datory.preprocessing.model.StatusModel;
import com.subhadip.datory.preprocessing.utils.GenericUtilities;
import com.subhadip.datory.preprocessing.utils.JSONUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants.STAGE_SUCCEEDED;
import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants._YES_;
import static org.apache.spark.launcher.SparkAppHandle.State.FINISHED;

public class Application implements HookFrameworkInterface {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    private Map<String, String> result =  new HashMap<String, String>(){
        {
            put("status", "-1");
            put("description", "Failed");

        } // default, pessimistic
    };


    public Map<String, String> execute(Map<String, String> args) {
        LOG.info("Starting Preprocessing Handler Client Application..");
        LOG.debug("Getting ENV..");
        LOG.debug(GenericUtilities.getEnvClasspath().toString());

        Seed seed = new Seed(args);
        try {
            seed.start();

            SparkJobLauncher job = new SparkJobLauncher(seed.getSparkJobParams());
            SparkAppHandle jobWatcher = job.launchSparkJob();


            if (jobWatcher.getState() == FINISHED) {
                StatusModel statusModel = getApplicationStatus(Seed.procInstanceId);

                if(statusModel == null) {
                    LOG.error(String.format("Spark Job Status= %s, " +
                            "But unable to retrieve cluster processing status.", jobWatcher.getState().toString())); // unable to retrieve output
                    LOG.info("Asking the Spark Application to STOP gracefully..");
                    jobWatcher.stop(); // try to ask the application to stop gracefully.
                    // This is best-effort, since the application may fail to receive or act on the command
                }

                else if (statusModel.getStatus().equals(STAGE_SUCCEEDED) // Success
                        && statusModel.getIsFinalStatus().equals(_YES_)) {

                    LOG.info("Pre-processing Successfully completed on Cluster..");
                    result.put("status", STAGE_SUCCEEDED);
                    result.put("description", new JSONUtils().HMapListToJSON(statusModel.getStageLogs()));
                }
            }
            else {
                LOG.error(String.format("Spark Job Watcher Timed-Out. Current Spark Job Status= %s " +
                        "Pre-processing Job on Spark failed. Check Spark Logs.", jobWatcher.getState().toString())); // timed-out

            }

        } catch (ApplicationException|JsonProcessingException e) {
            LOG.error(Throwables.getStackTraceAsString(e));
        }

        LOG.info(String.format("Final Application Status: %s", result)); // get the final status that would be sent to EDF
        return result;
    }


    private StatusModel getApplicationStatus(String procInstanceId) {
        LOG.info("Getting Application Status..");
        String jsonLogPathLocal = String.format("/tmp/%s/log.json", procInstanceId);

        LOG.info(String.format("Reading Local Log JSON : %s", jsonLogPathLocal));
        ObjectMapper objectMapper = new ObjectMapper();

        try(InputStream fileStream = new FileInputStream(jsonLogPathLocal)) {
            return objectMapper.readValue(fileStream, StatusModel.class);
        } catch (IOException e) {
            LOG.warn("Tried to retrieve Cluster application processing status from file: " + jsonLogPathLocal
                    + ", but could not access file. You may want to set higher " +
                    "\'client.spark.job.listener.timeout.secs\' value and Retry.");

            LOG.warn(Throwables.getStackTraceAsString(e));
            return null; // return null
        }
    }


    public static void main(String[] args) {
        new Application().execute(new HashMap<>()); // Only Stub. Not Used.
    }

    @Override
    public Map<String, String> invokeExtractIndexProcess(Map<String, HashMap<String, String>> map) {
        // From Solr indexing interface tied.
        return null;
    }
}