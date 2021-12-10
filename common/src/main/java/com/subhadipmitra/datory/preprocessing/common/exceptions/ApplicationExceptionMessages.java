package com.subhadipmitra.datory.preprocessing.common.exceptions;

public class ApplicationExceptionMessages {

    public static final String EXC_PARSING_COMMAND_LINE_ARGS = "Exception while parsing the command line arguments";
    public static final String EXC_FILE_NOT_FOUND_SPARK_PROPERTIES = "The spark property file \"{}\" not found. Check if the file exists and then run again";
    public static final String EXC_FILE_NOT_FOUND_APPLICATION_PROPERTIES = "The application property file \"{}\"  is not found. Check if the file exists and then run again";
    public static final String EXC_GENERIC = "Exception while running Dataory Preprocessing application";

    public static final String EXC_CONFIG_CLIENT_APP_JAR = "Pre-processing Application JAR path to run on cluster not set";
    public static final String EXC_CONFIG_CLIENT_APP_MAIN_CLASS = "Pre-processing Application JAR Main Class not provided";
    public static final String EXC_CONFIG_CLIENT_SPARK_MASTER = "Spark Master not set. Eg. yarn, local, local[*]";
    public static final String EXC_CONFIG_CLIENT_SPARK_DEPLOYMENT_MODE = "Spark Deployment Mode not set. Eg. client/cluster";
    public static final String EXC_CONFIG_CLIENT_SPARK_HOME = "SPARK_HOME path missing";
    public static final String EXC_CONFIG_CLIENT_SPARK_YARN_KEYTAB = "Spark Yarn Keytab not set";
    public static final String EXC_CONFIG_CLIENT_SPARK_YARN_PRINCIPAL = "Spark Yarn Principal not set. Eg. User Keytab/realm";

    public static final String EXC_CLIENT_CONVERT_STRING_TO_JSON = "Encountered error while converting args to JSON";
    public static final String EXC_CLIENT_MISSING_CALL_ARGS = "Missing arguments from hook invocation in application call: %s";

    public static final String EXC_CLIENT_UPDATE_CLIENT_CLUSTER_ARGS_TO_JSON = "Could not update Client Args to Cluster Args, encountered JSON processing exception";


}
