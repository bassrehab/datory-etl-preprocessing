package com.subhadipmitra.datory.preprocessing.client;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Throwables;
import com.subhadipmitra.datory.preprocessing.common.config.GenericConfigurationManager;
import com.subhadipmitra.datory.preprocessing.common.exceptions.ApplicationException;
import com.subhadip.datory.preprocessing.model.SparkJobModel;
import com.subhadip.datory.preprocessing.utils.JSONUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants.*;
import static com.subhadipmitra.datory.preprocessing.common.exceptions.ApplicationExceptionMessages.*;

class Seed {
    private static final Logger LOG = LoggerFactory.getLogger(Seed.class);

    private HashMap<String, String> args;
    private String argsJSON;
    public static String procInstanceId;
    private SparkJobModel sparkJobParams = new SparkJobModel();
    private GenericConfigurationManager config = new GenericConfigurationManager();
    private static final HashMap<String, String> paramMap = new HashMap<String, String>() {
        {
            put("i", "proc-id");
            put("d", "proc-date");
            put("n", "proc-instance-id");
            put("c", "proc-country-code");
            put("p", "skip-err-records-in-pre-proc");
            put("t", "timestamp");
            put("a", "application-properties-file");
        }
    };

    Seed(Map<String, String> args) {
        this.args = (HashMap<String, String>) args;
    }

    void start() throws ApplicationException {
        LOG.info("Starting seed process for client app ..");
        String missingArgs = validatedArguments();

        if(missingArgs == null) {
            procInstanceId = args.get("n"); // expose the procInstanceId in scope
            try {
                convertArgsToJSON();
                createSparkJobParams();

            } catch (JsonProcessingException e) {
                LOG.error(Throwables.getStackTraceAsString(e));
                throw new ApplicationException(EXC_CLIENT_CONVERT_STRING_TO_JSON, e);
            } catch (ApplicationException a){
                LOG.error(Throwables.getStackTraceAsString(a));
                throw new ApplicationException(a);

            }
        }
        else throw new ApplicationException(String.format(EXC_CLIENT_MISSING_CALL_ARGS, missingArgs));
    }

    private String validatedArguments() {
        HashSet<String> compareParams = new HashSet<>(paramMap.keySet());
        compareParams.removeAll(args.keySet());

        if(compareParams.size() > 0){
            // some args are missing, return missing args
            StringBuilder missingArgs = new StringBuilder();
            for(String arg : compareParams) missingArgs.append(", ").append(arg);
            return missingArgs.toString();
        }
        return null;
    }

    private void convertArgsToJSON() throws JsonProcessingException {
        argsJSON = new JSONUtils().HMapToJSON(args);
        LOG.info("Converted application args to JSON : " + argsJSON);
    }

    private void createSparkJobParams() throws ApplicationException {
        LOG.info("Creating SparkJob Params..");
        LOG.info(String.format("Going to read client application properties from : %s", args.get("a")));
        config.loadApplicationProperties(args.get("a")); // load the application properties file

        sparkJobParams.setServiceAppResource(config.getApplicationProperties(CLIENT_APP_JAR)
                .orElseThrow(() -> new ApplicationException(EXC_CONFIG_CLIENT_APP_JAR)));

        sparkJobParams.setServiceMainClass(config.getApplicationProperties(CLIENT_APP_MAIN_CLASS)
                .orElseThrow(() -> new ApplicationException(EXC_CONFIG_CLIENT_APP_MAIN_CLASS)));

        HashMap<String, String> updatedClusterAppArgs = args;
        updatedClusterAppArgs.replace("a", config.getApplicationProperties(CLIENT_CLUSTER_APPLICATION_PROPERTIES)
                .orElseThrow(() -> new ApplicationException(EXC_CONFIG_CLIENT_SPARK_MASTER)));

        try {
            sparkJobParams.setServiceAppArguments(new JSONUtils().HMapToJSON(updatedClusterAppArgs));
            LOG.info(String.format("Updated Client Args to Cluster Args: %s", sparkJobParams.getServiceAppArguments()));
        } catch (JsonProcessingException e) {
            LOG.error(Throwables.getStackTraceAsString(e));
            throw new ApplicationException(EXC_CLIENT_UPDATE_CLIENT_CLUSTER_ARGS_TO_JSON, e);

        }

        sparkJobParams.setSparkMaster(config.getApplicationProperties(CLIENT_SPARK_MASTER)
                .orElseThrow(() -> new ApplicationException(EXC_CONFIG_CLIENT_SPARK_MASTER)));

        sparkJobParams.setSparkDeploymentMode(config.getApplicationProperties(CLIENT_SPARK_DEPLOYMENT_MODE)
                .orElseThrow(() -> new ApplicationException(EXC_CONFIG_CLIENT_SPARK_DEPLOYMENT_MODE)));

        sparkJobParams.setSparkHome(config.getApplicationProperties(CLIENT_SPARK_HOME)
                .orElseThrow(() -> new ApplicationException(EXC_CONFIG_CLIENT_SPARK_HOME)));

        sparkJobParams.setSparkDriverMemory(config.getApplicationProperties(CLIENT_SPARK_DRIVER_MEMORY)
                .orElse(MEMORY_DEFAULT));

        sparkJobParams.setSparkExecutorMemory(config.getApplicationProperties(CLIENT_SPARK_EXECUTOR_MEMORY)
                .orElse(MEMORY_DEFAULT));

        sparkJobParams.setSparkExecutorCores(Integer.valueOf(config.getApplicationProperties(CLIENT_SPARK_EXECUTOR_CORES)
                .orElse(CPU_CORES_DEFAULT)));

        sparkJobParams.setSparkNumExecutors(Integer.valueOf(config.getApplicationProperties(CLIENT_SPARK_EXECUTOR_NUM)
                .orElse(CPU_CORES_DEFAULT)));

        sparkJobParams.setSparkSetVerbose(Boolean.parseBoolean(config.getApplicationProperties(CLIENT_SPARK_VERBOSE)
                .orElse(_TRUE_)));

        sparkJobParams.setSparkYarnKeytab(config.getApplicationProperties(CLIENT_SPARK_YARN_KEYTAB)
                .orElseThrow(() -> new ApplicationException(EXC_CONFIG_CLIENT_SPARK_YARN_KEYTAB)));

        sparkJobParams.setSparkYarnPrincipal(config.getApplicationProperties(CLIENT_SPARK_YARN_PRINCIPAL)
                .orElseThrow(() -> new ApplicationException(EXC_CONFIG_CLIENT_SPARK_YARN_PRINCIPAL)));

        sparkJobParams.setSparkAppName(config.getApplicationProperties(CLIENT_SPARK_APP_NAME)
                .orElse(APPLICATION_NAME));

        sparkJobParams.setSparkJars(config.getApplicationProperties(CLIENT_SPARK_EXTRA_JARS)
                .orElse(_EMPTY_STRING_));

        sparkJobParams.setSparkJobListenerTimeoutSecs(Integer.valueOf(
                config.getApplicationProperties(CLIENT_SPARK_JOB_LISTENER_TIMEOUT_SECS)
                .orElse(TIMEOUT_SECS_DEFAULT)));

        sparkJobParams.setSparkDriverJavaOptions(config.getApplicationProperties(CLIENT_SPARK_DRIVER_OPTIONS)
                .orElse(_EMPTY_STRING_));


        sparkJobParams.setSparkExecutorExtraJavaOptions(config.getApplicationProperties(CLIENT_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS)
                .orElse(_EMPTY_STRING_));

        sparkJobParams.setSparkExecutorExtraClasspath(config.getApplicationProperties(CLIENT_SPARK_EXECUTOR_EXTRA_CLASSPATH)
                .orElse(_EMPTY_STRING_));

        sparkJobParams.setSparkExecutorExtraLibraryPath(config.getApplicationProperties(CLIENT_SPARK_EXECUTOR_EXTRA_LIBRARY_PATH)
                .orElse(_EMPTY_STRING_));

        sparkJobParams.setSparkDriverExtraClasspath(config.getApplicationProperties(CLIENT_SPARK_DRIVER_EXTRA_CLASSPATH)
                .orElse(_EMPTY_STRING_));

        sparkJobParams.setSparkDriverExtraLibraryPath(config.getApplicationProperties(CLIENT_SPARK_DRIVER_EXTRA_LIBRARY_PATH)
                .orElse(_EMPTY_STRING_));


        String sparkSerializer = config.getApplicationProperties(CLIENT_SPARK_SERIALIZER)
                .orElse(SPARK_PROPERTY_CONF_SERIALIZER_KRYO_VALUE);

        sparkJobParams.setSparkSerializer(sparkSerializer);

        if(SPARK_PROPERTY_CONF_SERIALIZER_JAVA_VALUE.equals(sparkSerializer)){ // if its Java Serializer
            sparkJobParams.setJavaSerializerEnabled(Boolean.TRUE);
            sparkJobParams.setSparkSerializerObjectStreamReset(config.getApplicationProperties(CLIENT_SPARK_SERIALIZER_OBJECT_STREAM_RESET).orElse("100"));
        }
        else
            sparkJobParams.setSparkKryoSerializerBufferMax(config.getApplicationProperties(CLIENT_SPARK_KRYOSERIALIZER_BUFFER_MAX)
                    .orElse(KRYOSERIALIZER_BUFFER_MAX_DEFAULT));


        LOG.info("Generated SparkJob Params: " + sparkJobParams.toString());

    }

    private String getCurrentPackage() {
        return this.getClass().getPackage().getName();
    }

    public HashMap<String, String> getArgs() {
        return args;
    }

    public void setArgs(HashMap<String, String> args) {
        this.args = args;
    }

    public SparkJobModel getSparkJobParams() {
        return sparkJobParams;
    }

    public void setSparkJobParams(SparkJobModel sparkJobParams) {
        this.sparkJobParams = sparkJobParams;
    }
}
