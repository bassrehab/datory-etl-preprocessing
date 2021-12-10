package com.subhadip.datory.preprocessing.core.shared;

import com.subhadipmitra.datory.preprocessing.common.config.GenericConfigurationManager;
import com.subhadip.datory.preprocessing.model.PayloadModel;

import java.io.Serializable;
import java.util.HashMap;

import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants.IS_HOUSEKEEPING_ENABLED;

public class Parameters implements Serializable {

    private static final long serialVersionUID = 1315153197655797255L;

    public static boolean isSkipErrRecords;
    public static boolean isHousekeepingEnabled;
    public static HashMap<String, String> controlCharacterReplacementMap;

    public void load (PayloadModel payload){
        GenericConfigurationManager config = payload.getParamsModel().getConfig();
        isHousekeepingEnabled = Boolean.valueOf(config.getApplicationProperties(IS_HOUSEKEEPING_ENABLED).orElse("False"));
        isSkipErrRecords = payload.getParamsModel().isSkipErrorRecordsInPreProcessing();
        controlCharacterReplacementMap = payload.getSourceModel().getControlCharReplacementMap();

    }
}
