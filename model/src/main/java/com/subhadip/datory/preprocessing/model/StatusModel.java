package com.subhadip.datory.preprocessing.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StatusModel implements Serializable {

    private static final long serialVersionUID = -3776541893072954243L;
    private String status; // 0 = completed, -1 = failed, 1 = processing
    private HashMap<String, List<String>> StageLogs = new HashMap<>(); // stageId, stageLogs (string concatenated)
    private String isFinalStatus; //  started, processing, completed


    public HashMap<String, List<String>> getStageLogs() {
        return StageLogs;
    }

    public void setStageLogs(HashMap<String, List<String>> stageLogs) {
        StageLogs = stageLogs;
    }


    public synchronized void addToList(HashMap<String, List<String>> hm, String key, String value) {
        List<String> valueList = hm.get(key);
        if(valueList == null) {
            valueList = new ArrayList<>();
            valueList.add(value);
            hm.put(key, valueList);
        } else {
            if(!valueList.contains(value)) valueList.add(value);
        }
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getIsFinalStatus() {
        return isFinalStatus;
    }

    public void setIsFinalStatus(String isFinalStatus) {
        this.isFinalStatus = isFinalStatus;
    }
}
