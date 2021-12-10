package com.subhadipmitra.datory.preprocessing.client;

import com.google.common.base.Throwables;
import com.subhadipmitra.datory.preprocessing.common.exceptions.ApplicationException;
import com.subhadip.datory.preprocessing.model.SparkJobModel;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants.*;

class SparkJobLauncher {
    private static final Logger LOG = LoggerFactory.getLogger(SparkJobLauncher.class);
    private SparkJobModel params;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    SparkJobLauncher(SparkJobModel params) {
        this.params = params;
    }

    SparkAppHandle launchSparkJob() throws ApplicationException {
        SparkLauncher launcher = new SparkLauncher();
        SparkAppHandle handle;



        try {
            launcher
                    .setMainClass(params.getServiceMainClass())
                    .setAppResource(params.getServiceAppResource())
                    .addAppArgs(params.getServiceAppArguments()) //automatically generated.
                    .setMaster(params.getSparkMaster()) //yarn
                    .setSparkHome(params.getSparkHome())
                    .setDeployMode(params.getSparkDeploymentMode()) // cluster
                    .setConf(SPARK_PROPERTY_CONF_YARN_KEYTAB,params.getSparkYarnKeytab())
                    .setConf(SPARK_PROPERTY_CONF_YARN_PRINCIPAL,params.getSparkYarnPrincipal())
                    .setConf(SparkLauncher.DRIVER_MEMORY, params.getSparkDriverMemory())
                    .setConf(SparkLauncher.DRIVER_EXTRA_CLASSPATH, params.getSparkDriverExtraClasspath())
                    .setConf(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH, params.getSparkDriverExtraLibraryPath())
                    .setConf(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS, params.getSparkDriverJavaOptions())
                    .setConf(SparkLauncher.EXECUTOR_CORES, params.getSparkExecutorCores().toString())
                    .setConf(SparkLauncher.EXECUTOR_MEMORY, params.getSparkExecutorMemory())
                    .setConf(SparkLauncher.EXECUTOR_EXTRA_CLASSPATH, params.getSparkExecutorExtraClasspath())
                    .setConf(SparkLauncher.EXECUTOR_EXTRA_JAVA_OPTIONS, params.getSparkExecutorExtraJavaOptions())
                    .setConf(SparkLauncher.EXECUTOR_EXTRA_LIBRARY_PATH, params.getSparkExecutorExtraLibraryPath());


            // do kryo specific
            if(params.isJavaSerializerEnabled()){
                // do something specific to Java Serializer
                launcher.setConf(SPARK_PROPERTY_CONF_SERIALIZER, params.getSparkSerializer())
                        .setConf(SPARK_PROPERTY_CONF_JAVA_SERIALIZER_OBJECT_STREAM_RESET,
                                params.getSparkSerializerObjectStreamReset());
            }
            else launcher.setConf(SPARK_PROPERTY_CONF_KRYOSERIALIZER_BUFFER_MAX,
                    params.getSparkKryoSerializerBufferMax());

            handle = launcher.startApplication(new SparkAppHandle.Listener(){

                        @Override
                        public void infoChanged(SparkAppHandle handle) {
                            LOG.info("Spark App Id [" + handle.getAppId() + "] Info Changed.  State [" + handle.getState() + "]");
                        }

                        @Override
                        public void stateChanged(SparkAppHandle handle) {
                            // RUNNING, SUCCEEDED, FAILED, UNKNOWN
                            SparkAppHandle.State appState = handle.getState();
                            LOG.info("Spark App Id [" + handle.getAppId() + "] State Changed. State [" + appState + "]");

                            if (appState != null && appState.isFinal())
                                countDownLatch.countDown(); // waiting until spark driver exits
                        }
                    });
        } catch (IOException e) {
            LOG.error(Throwables.getStackTraceAsString(e));
            throw new ApplicationException("Encountered exception while launching Spark Job.", e);
        }

        LOG.info("Spark Job Launched [" + params.getServiceMainClass() + "] from [" + params.getServiceAppResource() + "] State [" + handle.getState() + "]");

        try {
            countDownLatch.await(params.getSparkJobListenerTimeoutSecs(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error(Throwables.getStackTraceAsString(e));
            throw new ApplicationException("Encountered InterruptedException while awaiting for Spark Job Listener", e);
        }
        return handle;
    }

}