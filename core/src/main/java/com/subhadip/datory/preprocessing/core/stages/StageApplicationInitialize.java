package com.subhadip.datory.preprocessing.core.stages;
import com.google.common.base.Throwables;
import com.subhadip.datory.preprocessing.core.mapper.ArgsMapper;
import com.subhadipmitra.datory.preprocessing.common.config.GenericConfigurationManager;
import com.subhadipmitra.datory.preprocessing.common.connection.MetadataConnection;
import com.subhadipmitra.datory.preprocessing.common.exceptions.ApplicationException;
import com.subhadip.datory.preprocessing.model.PayloadModel;
import com.subhadip.datory.preprocessing.pipeline.StageInterface;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants.*;

public class StageApplicationInitialize implements StageInterface<PayloadModel, PayloadModel>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StageApplicationInitialize.class);
    private static final long serialVersionUID = -2760681348780094052L;
    private PayloadModel payload;

    private final String STAGE_NAME = this.getClass().getCanonicalName();

    @Override
    public PayloadModel run(PayloadModel payload) {
        LOG.info("Starting Stage 1: Application Initialize");

        this.payload = payload;
        payload.getStatusModel().setStatus(STAGE_PROCESSING); // started processing
        payload.getStatusModel().setIsFinalStatus(_NO_); // this is not final status


        try{
            parseArgsJSON(payload.getParamsModel().getArgsJSON());

            // Generate Config Obj
            GenericConfigurationManager config = payload.getParamsModel().getConfig();
            config.loadApplicationProperties(payload.getParamsModel().getApplicationPropsFile()); // load config file
            payload.getParamsModel().setConfig(config);

            // Metadata DB Conn obj
            payload.getParamsModel().setMetaDbConn(new MetadataConnection(payload.getParamsModel().getConfig()));
            payload.getStatusModel().addToList(payload.getStatusModel().getStageLogs(), STAGE_NAME, STAGE_SUCCEEDED);

        }
        catch(ApplicationException e){
            LOG.error(Throwables.getStackTraceAsString(e));
            payload.getStatusModel().setStatus(STAGE_FAILED);
            payload.getStatusModel().setIsFinalStatus(_YES_);
            payload.getStatusModel().addToList(payload.getStatusModel().getStageLogs(), STAGE_NAME, Throwables.getStackTraceAsString(e));
            // Will collect in next stage, config init is failed, prolly no-use of flush to DB.
        }
        return payload;
    }


    private void parseArgsJSON(String json) throws ApplicationException {
        ObjectMapper mapper = new ObjectMapper();

        try {
            ArgsMapper argMapper = mapper.readValue(json, ArgsMapper.class);

            payload.getParamsModel().setProcId(argMapper.getI());
            payload.getParamsModel().setProcDate(argMapper.getD());
            payload.getParamsModel().setProcInstanceId(argMapper.getN());
            payload.getParamsModel().setProcCtryCd(argMapper.getC());
            payload.getParamsModel().setSkipErrorRecordsInPreProcessing(Boolean.valueOf(argMapper.getP()));
            payload.getParamsModel().setApplicationPropsFile(argMapper.getA());
            payload.getParamsModel().setTimeStamp(argMapper.getT());
        }
        catch(IOException e){
            LOG.error(Throwables.getStackTraceAsString(e));
            throw new ApplicationException("Encountered exception while trying to convert JSON Args to Java Object", e);
        }

    }
}
