

package com.subhadip.datory.preprocessing.model;

import com.subhadipmitra.datory.preprocessing.common.config.GenericConfigurationManager;
import com.subhadipmitra.datory.preprocessing.common.connection.MetadataConnection;

import java.io.Serializable;

// Model to Capture the Command Line Arguments
public class ParamsModel implements Serializable {

    private static final long serialVersionUID = -1504661629323918306L;
    private String procId;
    private String procDate;
    private String procCtryCd;
    private String procInstanceId;
    private boolean isSkipErrorRecordsInPreProcessing;
    private String timeStamp;
    private String applicationPropsFile;
    private GenericConfigurationManager config;
    private MetadataConnection metaDbConn;
    private String argsJSON;


    ParamsModel() {
        this.setConfig(new GenericConfigurationManager());
    }

    public String getProcId() {
        return procId;
    }

    public void setProcId(String procId) {
        this.procId = procId;
    }

    public String getProcDate() {
        return procDate;
    }

    public void setProcDate(String procDate) {
        this.procDate = procDate;
    }

    public String getProcCtryCd() {
        return procCtryCd;
    }

    public void setProcCtryCd(String procCtryCd) {
        this.procCtryCd = procCtryCd;
    }

    public String getProcInstanceId() {
        return procInstanceId;
    }

    public void setProcInstanceId(String procInstanceId) {
        this.procInstanceId = procInstanceId;
    }

    public boolean isSkipErrorRecordsInPreProcessing() {
        return isSkipErrorRecordsInPreProcessing;
    }

    public void setSkipErrorRecordsInPreProcessing(boolean skipErrorRecordsInPreProcessing) {
        this.isSkipErrorRecordsInPreProcessing = skipErrorRecordsInPreProcessing;
    }

    public String getApplicationPropsFile() {
        return applicationPropsFile;
    }

    public void setApplicationPropsFile(String applicationPropsFile) {
        this.applicationPropsFile = applicationPropsFile;
    }

    public GenericConfigurationManager getConfig() {
        return config;
    }

    public void setConfig(GenericConfigurationManager config) {
        this.config = config;
    }

    public MetadataConnection getMetaDbConn() {
        return metaDbConn;
    }

    public void setMetaDbConn(MetadataConnection metaDbConn) {
        this.metaDbConn = metaDbConn;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getArgsJSON() {
        return argsJSON;
    }

    public void setArgsJSON(String argsJSON) {
        this.argsJSON = argsJSON;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }
}
