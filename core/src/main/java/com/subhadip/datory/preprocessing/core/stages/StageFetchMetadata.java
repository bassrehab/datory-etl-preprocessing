package com.subhadip.datory.preprocessing.core.stages;

import com.google.common.base.Throwables;
import com.subhadipmitra.datory.preprocessing.common.exceptions.ApplicationException;
import com.subhadip.datory.preprocessing.core.metadata.PreProcessingDAO;
import com.subhadip.datory.preprocessing.model.PayloadModel;
import com.subhadip.datory.preprocessing.pipeline.StageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;

import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants.*;

public class StageFetchMetadata implements StageInterface<PayloadModel, PayloadModel>, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(StageFetchMetadata.class);
    private static final long serialVersionUID = 246997339616105306L;
    private final String STAGE_NAME = this.getClass().getCanonicalName();

    @Override
    public PayloadModel run(PayloadModel payload) {
        LOG.info("Starting State 2: Fetch Metadata");

        PreProcessingDAO metadata = new PreProcessingDAO(payload.getParamsModel().getMetaDbConn());

        try {
            payload = metadata.getFileDetails(payload);
            payload = metadata.getCtryCtrlCharacterReplacement(payload);
            payload = metadata.getDestinationDetails(payload);
            payload = metadata.getNumberOfColumns(payload);
            payload.getStatusModel().addToList(payload.getStatusModel().getStageLogs(), STAGE_NAME, STAGE_SUCCEEDED);

        } catch (SQLException | ApplicationException e) {
            LOG.error(Throwables.getStackTraceAsString(e));
            payload.getStatusModel().setStatus(STAGE_FAILED);
            payload.getStatusModel().setIsFinalStatus(_YES_);
            payload.getStatusModel().addToList(payload.getStatusModel().getStageLogs(), STAGE_NAME, Throwables.getStackTraceAsString(e));
        }
        return payload;
    }

}
