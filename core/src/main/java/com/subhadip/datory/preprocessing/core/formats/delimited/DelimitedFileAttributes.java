package com.subhadip.datory.preprocessing.core.formats.delimited;

import com.subhadipmitra.datory.preprocessing.common.config.GenericConfigurationManager;
import com.subhadip.datory.preprocessing.model.PayloadModel;
import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants.*;

public class DelimitedFileAttributes implements Serializable {

    private static final long serialVersionUID = 2165045898226810895L;
    private static final Logger LOG = LoggerFactory.getLogger(DelimitedFileAttributes.class);

    public static String delimitedRecordEnd;
    public static Character delimitedRecordEndChar = '\n';

    public static String delimitedRecordStart;
    public static Character delimitedRecordStartChar = 'D';

    public static String delimitedTrailerStart;
    public static Character delimitedTrailerStartChar = 'T';

    public static String delimitedRecordColumnDelimiter;
    public static Character delimitedRecordColumnDelimiterChar ='\u0007';
    public static int delimitedExpectedColsNum;
    public static int delimitedMaxLineLength = Integer.MAX_VALUE;

    public void load (PayloadModel payload){
        GenericConfigurationManager config = payload.getParamsModel().getConfig();

        delimitedRecordEnd = config.getApplicationProperties(DELIMITED_RECORD_END_CHAR).orElse("\n");
        LOG.info("delimitedRecordEnd: " + config.getApplicationProperties(DELIMITED_RECORD_END_CHAR));


        delimitedRecordStart = config.getApplicationProperties(DELIMITED_RECORD_START_CHAR).orElse("D");
        LOG.info("delimitedRecordStart: " + config.getApplicationProperties(DELIMITED_RECORD_START_CHAR));


        delimitedTrailerStart = config.getApplicationProperties(DELIMITED_TRAILER_START_CHAR).orElse("T");
        LOG.info("delimitedTrailerStart: " + config.getApplicationProperties(DELIMITED_TRAILER_START_CHAR));


        delimitedRecordColumnDelimiter = config.getApplicationProperties(DELIMITED_COLUMN_CHAR).orElse("\u0007");
        LOG.info("delimitedRecordColumnDelimiter: " + config.getApplicationProperties(DELIMITED_COLUMN_CHAR));


        delimitedExpectedColsNum = payload.getLayoutModel().getDelimited().getNumberOfColumns();
        delimitedMaxLineLength = Integer.parseInt(config.getApplicationProperties(DELIMITED_MAX_LINE_LENGTH).orElse(String.valueOf(Integer.MAX_VALUE)));


        delimitedRecordStartChar = StringEscapeUtils.unescapeJava(delimitedRecordStart).charAt(0);
        delimitedRecordEndChar = delimitedRecordEnd.charAt(0);
        delimitedTrailerStartChar = StringEscapeUtils.unescapeJava(delimitedRecordStart).charAt(0);
        delimitedRecordColumnDelimiterChar = StringEscapeUtils.unescapeJava(delimitedRecordColumnDelimiter).charAt(0);
    }
}
