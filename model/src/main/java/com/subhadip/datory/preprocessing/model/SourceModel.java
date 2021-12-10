package com.subhadip.datory.preprocessing.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SourceModel implements Serializable {

    private static final long serialVersionUID = -1982931196932077024L;
    private String landingFolder;
    private String landingFileName;

    public String getArchiveFolder() {
        return archiveFolder;
    }

    public void setArchiveFolder(String archiveFolder) {
        this.archiveFolder = archiveFolder;
    }

    private String archiveFolder;

    private int fileId;
    private HashMap<String, String> controlCharReplacementMap;

    public String getLandingFolder() {
        return landingFolder;
    }

    public void setLandingFolder(String landingFolder) {
        landingFolder = String.format("%sprocessing/", landingFolder.replace(getLandingFileName(), ""));
        this.landingFolder = landingFolder;
    }

    public String getLandingFileName() {
        return landingFileName;
    }

    public void setLandingFileName(String landingFileName) {
        this.landingFileName = landingFileName;
    }

    public int getFileId() {
        return fileId;
    }

    public void setFileId(int fileId) {
        this.fileId = fileId;
    }


    public HashMap<String, String> getControlCharReplacementMap() {
        return controlCharReplacementMap;
    }


    public void setControlCharReplacementMap(HashMap<String, String> controlCharReplacementMap) {
        this.controlCharReplacementMap = controlCharReplacementMap;
    }

    public void setControlCharReplacementMap(String k, String v){
        HashMap<String, String> map= new HashMap<>();
        map.put(k, v);
        this.controlCharReplacementMap = map;
    }

}

