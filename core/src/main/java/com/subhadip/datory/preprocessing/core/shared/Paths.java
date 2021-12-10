package com.subhadip.datory.preprocessing.core.shared;

import com.subhadipmitra.datory.preprocessing.common.config.GenericConfigurationManager;
import com.subhadip.datory.preprocessing.model.PayloadModel;

import java.io.Serializable;

import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants.*;

public class Paths implements Serializable {
    private static final long serialVersionUID = 9035538592243934915L;

    /*
     * ----------------------------------------------------------------------|
     *   HDFS Working Directory                                              |
     *   hdfs://tmp/<proc_id>/<proc_instance_id>/                            |
     *   hdfsWorkingDir                                                      |
     *   --------------------------------------------------------------------|
     *                                                                       |
     *   HDFS where input file will be deposited from Landing Folder         |
     *   hdfs://tmp/<proc_id>/<proc_instance_id>/input                       |
     *   inputFilePathHdfs                                                   |
     *   --------------------------------------------------------------------|
     *                                                                       |
     *   HDFS - sub dir /clean where Clean RDD will be flushed               |
     *   hdfs://tmp/<proc_id>/<proc_instance_id>/temp/clean/part-00000       |
     *   hdfs://tmp/<proc_id>/<proc_instance_id>/temp/clean/                 |
     *                                                                       |
     *   --------------------------------------------------------------------|
     *                                                                       |
     *   HDFS - sub dir /clean where Bad RDD will be flushed                 |
     *   hdfs://tmp/<proc_id>/<proc_instance_id>/temp/bad/part-00000         |
     *   hdfs://tmp/<proc_id>/<proc_instance_id>/temp/bad/                   |
     *                                                                       |
     *   --------------------------------------------------------------------|
     *                                                                       |
     *   HDFS Output Dir where the final files will be                       |
     *   hdfs://tmp/<proc_id>/<proc_instance_id>/output/clean                |
     *   hdfs://tmp/<proc_id>/<proc_instance_id>/output/bad                  |
     *                                                                       |
     * ----------------------------------------------------------------------|
     */

    public static String hdfsWorkingDir;
    public static String subDirTemp;
    public static String fileNameDateTimestamp;
    public static String inputFilePathLocal;
    public static String inputFilePathHdfs;
    public static String tempDirPathHdfs; // Dir: /tmp,  dir where custom input format rdd is flushed, part files
    public static String tempFilePathHdfsCombineClean; // File /tmp/combined
    public static String tempFilePathHdfsCombineBad; // File /tmp/combined

    public static String tempDirHdfsClean; // File: tmp/clean
    public static String tempDirHdfsBad; // File: tmp/bad
    public static String outputFilePathLocalClean;
    public static String outputFilePathLocalBad;

    public static String logPathHDFS;
    public static String logPathLocal;


    public void load (PayloadModel payload){

        GenericConfigurationManager config = payload.getParamsModel().getConfig();

        hdfsWorkingDir = config.getApplicationProperties(COMMON_WORKING_DIR).orElse("hdfs:///tmp/")
                                    + payload.getParamsModel().getProcId() + _SLASH_
                                    + payload.getParamsModel().getProcInstanceId() + _SLASH_;

        subDirTemp = config.getApplicationProperties(COMMON_WORKING_DIR_SUB).orElse("/tmp/");


        // file ops
        String inputInstanceFileName = payload.getSourceModel().getLandingFileName() +  _DOT_
                + payload.getParamsModel().getProcDate() + _DOT_ + payload.getParamsModel().getTimeStamp();

        String str = payload.getSourceModel().getLandingFolder()
                .replaceAll(payload.getSourceModel().getLandingFileName(),_EMPTY_STRING_);

        inputFilePathLocal = str
                + inputInstanceFileName; // file

        inputFilePathLocal = inputFilePathLocal.replaceAll("<ctry_cd>", payload.getParamsModel().getProcCtryCd().toLowerCase());
        inputFilePathHdfs = hdfsWorkingDir + inputInstanceFileName; // file

        tempDirPathHdfs = hdfsWorkingDir + subDirTemp; //dir, where custom input format rdd is flushed, part files

        tempFilePathHdfsCombineClean = tempDirPathHdfs + "/combine_clean"; // combined file
        tempFilePathHdfsCombineBad = tempDirPathHdfs + "/combine_bad"; // combined file

        tempDirHdfsClean = tempDirPathHdfs + "/clean/"; // part-xxxxxx file location dir
        tempDirHdfsBad = tempDirPathHdfs + "/bad/";     // part-xxxxx file location dir

        outputFilePathLocalClean = inputFilePathLocal + _CLEAN_FILE_SUFFIX;
        outputFilePathLocalBad = outputFilePathLocalClean + _ERROR_FILE_SUFFIX;

        logPathHDFS = hdfsWorkingDir + "log.json";
        logPathLocal = "/tmp/" + payload.getParamsModel().getProcInstanceId() + "/log.json";

    }
}
