package com.subhadip.datory.preprocessing.model;

import java.io.Serializable;

public class SparkJobModel implements Serializable {

    private static final long serialVersionUID = -6906479112292196569L;
    private String serviceAppResource; //path to JAR.
    private String serviceMainClass; // JAR MainClass to be Executed
    private String serviceAppArguments; // JAR native args.

    private String sparkMaster;
    private String sparkDeploymentMode;
    private String sparkHome;
    private String sparkDriverMemory;
    private String sparkExecutorMemory;
    private Integer sparkExecutorCores;
    private Integer sparkNumExecutors;
    private boolean sparkSetVerbose = Boolean.TRUE;
    private String sparkYarnKeytab;
    private String sparkYarnPrincipal;
    private String sparkAppName;
    private String sparkJars;
    private String sparkKryoSerializerBufferMax;

    private String sparkExecutorExtraJavaOptions;
    private String sparkExecutorExtraClasspath;
    private String sparkExecutorExtraLibraryPath;

    private String sparkDriverExtraClasspath;
    private String sparkDriverExtraLibraryPath;

    private String sparkSerializer;
    private String sparkSerializerObjectStreamReset;

    private boolean isJavaSerializerEnabled = Boolean.FALSE;

    public String getSparkDriverJavaOptions() {
        return sparkDriverJavaOptions;
    }

    public void setSparkDriverJavaOptions(String sparkDriverJavaOptions) {
        this.sparkDriverJavaOptions = sparkDriverJavaOptions;
    }

    private String sparkDriverJavaOptions; // jaas.conf
    private Integer sparkJobListenerTimeoutSecs = 120;

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public Integer getSparkJobListenerTimeoutSecs() {
        return sparkJobListenerTimeoutSecs;
    }

    public void setSparkJobListenerTimeoutSecs(Integer sparkJobListenerTimeoutSecs) {
        this.sparkJobListenerTimeoutSecs = sparkJobListenerTimeoutSecs;
    }

    public String getServiceAppResource() {
        return serviceAppResource;
    }

    public void setServiceAppResource(String serviceAppResource) {
        this.serviceAppResource = serviceAppResource;
    }

    public String getServiceMainClass() {
        return serviceMainClass;
    }

    public void setServiceMainClass(String serviceMainClass) {
        this.serviceMainClass = serviceMainClass;
    }

    public String getServiceAppArguments() {
        return serviceAppArguments;
    }

    public void setServiceAppArguments(String serviceAppArguments) {
        this.serviceAppArguments = serviceAppArguments;
    }

    public String getSparkMaster() {
        return sparkMaster;
    }

    public void setSparkMaster(String sparkMaster) {
        this.sparkMaster = sparkMaster;
    }

    public String getSparkDeploymentMode() {
        return sparkDeploymentMode;
    }

    public void setSparkDeploymentMode(String sparkDeploymentMode) {
        this.sparkDeploymentMode = sparkDeploymentMode;
    }

    public String getSparkHome() {
        return sparkHome;
    }

    public void setSparkHome(String sparkHome) {
        this.sparkHome = sparkHome;
    }

    public String getSparkDriverMemory() {
        return sparkDriverMemory;
    }

    public void setSparkDriverMemory(String sparkDriverMemory) {
        this.sparkDriverMemory = sparkDriverMemory;
    }

    public String getSparkExecutorMemory() {
        return sparkExecutorMemory;
    }

    public void setSparkExecutorMemory(String sparkExecutorMemory) {
        this.sparkExecutorMemory = sparkExecutorMemory;
    }

    public Integer getSparkExecutorCores() {
        return sparkExecutorCores;
    }

    public void setSparkExecutorCores(Integer sparkExecutorCores) {
        this.sparkExecutorCores = sparkExecutorCores;
    }

    public Integer getSparkNumExecutors() {
        return sparkNumExecutors;
    }

    public void setSparkNumExecutors(Integer sparkNumExecutors) {
        this.sparkNumExecutors = sparkNumExecutors;
    }

    public boolean isSparkSetVerbose() {
        return sparkSetVerbose;
    }

    public void setSparkSetVerbose(boolean sparkSetVerbose) {
        this.sparkSetVerbose = sparkSetVerbose;
    }

    public String getSparkYarnKeytab() {
        return sparkYarnKeytab;
    }

    public void setSparkYarnKeytab(String sparkYarnKeytab) {
        this.sparkYarnKeytab = sparkYarnKeytab;
    }

    public String getSparkYarnPrincipal() {
        return sparkYarnPrincipal;
    }

    public void setSparkYarnPrincipal(String sparkYarnPrincipal) {
        this.sparkYarnPrincipal = sparkYarnPrincipal;
    }

    public String getSparkAppName() {
        return sparkAppName;
    }

    public void setSparkAppName(String sparkAppName) {
        this.sparkAppName = sparkAppName;
    }

    public String getSparkJars() {
        return sparkJars;
    }

    public void setSparkJars(String sparkJars) {
        this.sparkJars = sparkJars;
    }

    public String getSparkKryoSerializerBufferMax() {
        return sparkKryoSerializerBufferMax;
    }

    public void setSparkKryoSerializerBufferMax(String sparkKryoSerializerBufferMax) {
        this.sparkKryoSerializerBufferMax = sparkKryoSerializerBufferMax;
    }

    public String getSparkExecutorExtraJavaOptions() {
        return sparkExecutorExtraJavaOptions;
    }

    public void setSparkExecutorExtraJavaOptions(String sparkExecutorExtraJavaOptions) {
        this.sparkExecutorExtraJavaOptions = sparkExecutorExtraJavaOptions;
    }

    public String getSparkExecutorExtraClasspath() {
        return sparkExecutorExtraClasspath;
    }

    public void setSparkExecutorExtraClasspath(String sparkExecutorExtraClasspath) {
        this.sparkExecutorExtraClasspath = sparkExecutorExtraClasspath;
    }

    public String getSparkExecutorExtraLibraryPath() {
        return sparkExecutorExtraLibraryPath;
    }

    public void setSparkExecutorExtraLibraryPath(String sparkExecutorExtraLibraryPath) {
        this.sparkExecutorExtraLibraryPath = sparkExecutorExtraLibraryPath;
    }

    public String getSparkDriverExtraClasspath() {
        return sparkDriverExtraClasspath;
    }

    public void setSparkDriverExtraClasspath(String sparkDriverExtraClasspath) {
        this.sparkDriverExtraClasspath = sparkDriverExtraClasspath;
    }

    public String getSparkDriverExtraLibraryPath() {
        return sparkDriverExtraLibraryPath;
    }

    public void setSparkDriverExtraLibraryPath(String sparkDriverExtraLibraryPath) {
        this.sparkDriverExtraLibraryPath = sparkDriverExtraLibraryPath;
    }

    public String getSparkSerializer() {
        return sparkSerializer;
    }

    public void setSparkSerializer(String sparkSerializer) {
        this.sparkSerializer = sparkSerializer;
    }

    public String getSparkSerializerObjectStreamReset() {
        return sparkSerializerObjectStreamReset;
    }

    public void setSparkSerializerObjectStreamReset(String sparkSerializerObjectStreamReset) {
        this.sparkSerializerObjectStreamReset = sparkSerializerObjectStreamReset;
    }

    public boolean isJavaSerializerEnabled() {
        return isJavaSerializerEnabled;
    }

    public void setJavaSerializerEnabled(boolean javaSerializerEnabled) {
        isJavaSerializerEnabled = javaSerializerEnabled;
    }


    @Override
    public String toString() {
        return "SparkJobModel{" +
                "serviceAppResource='" + serviceAppResource + '\'' +
                ", serviceMainClass='" + serviceMainClass + '\'' +
                ", serviceAppArguments='" + serviceAppArguments + '\'' +
                ", sparkMaster='" + sparkMaster + '\'' +
                ", sparkDeploymentMode='" + sparkDeploymentMode + '\'' +
                ", sparkHome='" + sparkHome + '\'' +
                ", sparkDriverMemory='" + sparkDriverMemory + '\'' +
                ", sparkExecutorMemory='" + sparkExecutorMemory + '\'' +
                ", sparkExecutorCores=" + sparkExecutorCores +
                ", sparkNumExecutors=" + sparkNumExecutors +
                ", sparkSetVerbose=" + sparkSetVerbose +
                ", sparkYarnKeytab='" + sparkYarnKeytab + '\'' +
                ", sparkYarnPrincipal='" + sparkYarnPrincipal + '\'' +
                ", sparkAppName='" + sparkAppName + '\'' +
                ", sparkJars='" + sparkJars + '\'' +
                ", sparkKryoSerializerBufferMax='" + sparkKryoSerializerBufferMax + '\'' +
                ", sparkExecutorExtraJavaOptions='" + sparkExecutorExtraJavaOptions + '\'' +
                ", sparkExecutorExtraClasspath='" + sparkExecutorExtraClasspath + '\'' +
                ", sparkExecutorExtraLibraryPath='" + sparkExecutorExtraLibraryPath + '\'' +
                ", sparkDriverExtraClasspath='" + sparkDriverExtraClasspath + '\'' +
                ", sparkDriverExtraLibraryPath='" + sparkDriverExtraLibraryPath + '\'' +
                ", sparkSerializer='" + sparkSerializer + '\'' +
                ", sparkSerializerObjectStreamReset='" + sparkSerializerObjectStreamReset + '\'' +
                ", isJavaSerializerEnabled=" + isJavaSerializerEnabled +
                ", sparkDriverJavaOptions='" + sparkDriverJavaOptions + '\'' +
                ", sparkJobListenerTimeoutSecs=" + sparkJobListenerTimeoutSecs +
                '}';
    }
}
