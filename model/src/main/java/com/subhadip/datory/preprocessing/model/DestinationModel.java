package com.subhadip.datory.preprocessing.model;

import java.io.Serializable;

public class DestinationModel implements Serializable {

    private static final long serialVersionUID = 828558313743226038L;
    private String stagingDirName;
    private String stagingDbName;
    private String stagingTableName;
    private String stagingTablePartitionText;

    public String getStagingDirName() {
        return stagingDirName;
    }

    public void setStagingDirName(String stagingDirName) {
        this.stagingDirName = stagingDirName;
    }

    public String getStagingDbName() {
        return stagingDbName;
    }

    public void setStagingDbName(String stagingDbName) {
        this.stagingDbName = stagingDbName;
    }


    public String getStagingTableName() {
        return stagingTableName;
    }

    public void setStagingTableName(String stagingTableName) {
        this.stagingTableName = stagingTableName;
    }

    public String getStagingTablePartitionText() {
        return stagingTablePartitionText;
    }

    public void setStagingTablePartitionText(String stagingTablePartitionText) {
        this.stagingTablePartitionText = stagingTablePartitionText;
    }
}


