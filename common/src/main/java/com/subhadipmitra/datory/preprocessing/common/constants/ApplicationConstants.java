package com.subhadipmitra.datory.preprocessing.common.constants;

public class ApplicationConstants {

    public static final String _EMPTY_STRING_ = "";
    public static final String _DOT_ =".";
    public static final String _SLASH_ = "/";
    public static final String _DATA_ROW_= "9055dede-3b7c-425c-91a1-2fc47f8c5536"; // just some tags, RFC 4122 compliant
    public static final String _CLEAN_FILE_SUFFIX = "_1";
    public static final String _ERROR_FILE_SUFFIX = "_0";
    public static final String _NULL_LITERAL_ ="null";
    public static final String _TRUE_ = "true";

    // Application Properties
    public static final String METADATA_DATABASE_DRIVER = "metadata-database-driver";
    public static final String METADATA_JDBC_CONNECTION_STRING = "metadata-jdbc-connection-string";
    public static final String METADATA_DATABASE_USERNAME = "metadata-database-username";
    public static final String METADATA_DATABASE_PASSWORD = "metadata-database-password";


    //-------

    public static final String APPLICATION_NAME = "Datory ETL Pre-Processing Application";
    public static final String LAYOUT_CODE_DELIMITED = "DEL";
    public static final String LAYOUT_CODE_FIXEDWIDTH = "FXD";
    public static final String _NEW_LINE_ = "\n";
    public static final String _SPACE_ = " ";
    public static final int F_SUCCESS = 0;
    public static final int F_FAILURE = -1;
    public static final String _YES_ = "Y";
    public static final String _NO_ = "N";

    public static final String STAGE_SUCCEEDED = "0";
    public static final String STAGE_FAILED = "-1";
    public static final String STAGE_UNDEFINED = "-100";
    public static final String STAGE_PROCESSING = "1";


    //--------------------------------------


    public static final String CLIENT_APP_JAR = "client.app.jar";
    public static final String CLIENT_APP_MAIN_CLASS = "client.app.main.class";
    public static final String CLIENT_APP_ARGS = "client.app.args";
    public static final String CLIENT_SPARK_APP_NAME = "client.spark.app.name";
    public static final String CLIENT_SPARK_MASTER = "client.spark.master";
    public static final String CLIENT_SPARK_DEPLOYMENT_MODE = "client.spark.deployment.mode";
    public static final String CLIENT_SPARK_HOME = "client.spark.home";
    public static final String CLIENT_SPARK_DRIVER_MEMORY = "client.spark.driver.memory";
    public static final String CLIENT_SPARK_EXECUTOR_MEMORY = "client.spark.executor.memory";
    public static final String CLIENT_SPARK_EXECUTOR_CORES = "client.spark.executor.cores";
    public static final String CLIENT_SPARK_EXECUTOR_NUM = "client.spark.executor.num";
    public static final String CLIENT_SPARK_VERBOSE = "client.spark.verbose";
    public static final String CLIENT_SPARK_YARN_KEYTAB = "client.spark.yarn.keytab";
    public static final String CLIENT_SPARK_YARN_PRINCIPAL = "client.spark.yarn.principal";
    public static final String CLIENT_SPARK_EXTRA_JARS = "client.spark.extra.jars";
    public static final String CLIENT_SPARK_JOB_LISTENER_TIMEOUT_SECS = "client.spark.job.listener.timeout.secs";
    public static final String CLIENT_CLUSTER_APPLICATION_PROPERTIES = "client.cluster.application.properties";
    public static final String CLIENT_SPARK_DRIVER_OPTIONS = "client.spark.driver.java.options";
    public static final String PROPERTY_SPARK_DRIVER_OPTIONS="--driver-java-options";
    public static final String CLIENT_SPARK_KRYOSERIALIZER_BUFFER_MAX="client.spark.kryoserializer.buffer.max";

    public static final String CLIENT_SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS="client.spark.executor.extra.java.options";
    public static final String CLIENT_SPARK_EXECUTOR_EXTRA_CLASSPATH="client.spark.executor.extra.classpath";
    public static final String CLIENT_SPARK_EXECUTOR_EXTRA_LIBRARY_PATH="client.spark.executor.extra.library.path";
    public static final String CLIENT_SPARK_DRIVER_EXTRA_CLASSPATH="client.spark.driver.extra.classpath";
    public static final String CLIENT_SPARK_DRIVER_EXTRA_LIBRARY_PATH="client.spark.driver.extra.library.path";

    public static final String CLIENT_SPARK_SERIALIZER = "client.spark.serializer";
    public static final String CLIENT_SPARK_SERIALIZER_OBJECT_STREAM_RESET = "client.spark.serializer.objectStreamReset";

    public static final String DELIMITED_RECORD_END_CHAR = "core.fileformat.delimited.record.end.char";
    public static final String DELIMITED_RECORD_START_CHAR = "core.fileformat.delimited.record.start.char";
    public static final String DELIMITED_TRAILER_START_CHAR = "core.fileformat.delimited.trailer.start.char";


    public static final String DELIMITED_COLUMN_CHAR = "core.fileformat.delimited.record.column.char";
    public static final String DELIMITED_MAX_LINE_LENGTH = "core.fileformat.delimited.max.line.length";

    public static final String COMMON_WORKING_DIR = "core.fileformat.common.workingdir";
    public static final String COMMON_WORKING_DIR_SUB ="core.fileformat.common.workingdir.sub";
    public static final String IS_HOUSEKEEPING_ENABLED = "core.enable.housekeeping";


    public static final String MEMORY_DEFAULT = "4g";
    public static final String CPU_CORES_DEFAULT = "4";
    public static final String TIMEOUT_SECS_DEFAULT = "120";
    public static final String KRYOSERIALIZER_BUFFER_MAX_DEFAULT = "1024m";

    public static final String SPARK_PROPERTY_CONF_YARN_KEYTAB = "spark.yarn.keytab";
    public static final String SPARK_PROPERTY_CONF_YARN_PRINCIPAL = "spark.yarn.keytab";
    public static final String SPARK_PROPERTY_CONF_KRYOSERIALIZER_BUFFER_MAX = "spark.kryoserializer.buffer.max";
    public static final String SPARK_PROPERTY_CONF_SERIALIZER = "spark.serializer";
    public static final String SPARK_PROPERTY_CONF_JAVA_SERIALIZER_OBJECT_STREAM_RESET = "spark.serializer.objectStreamReset";

    public static final String SPARK_PROPERTY_CONF_SERIALIZER_JAVA_VALUE = "org.apache.serializer.JavaSerializer";
    public static final String SPARK_PROPERTY_CONF_SERIALIZER_KRYO_VALUE = "org.apache.spark.serializer.KryoSerializer";



}