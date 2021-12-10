package com.subhadip.datory.preprocessing.core.stages;

import com.google.common.base.Throwables;
import com.subhadipmitra.datory.preprocessing.common.exceptions.ApplicationException;
import com.subhadip.datory.preprocessing.core.formats.delimited.DelimitedFileAttributes;
import com.subhadip.datory.preprocessing.core.formats.delimited.DelimitedFileDriver;
import com.subhadip.datory.preprocessing.core.shared.Parameters;
import com.subhadip.datory.preprocessing.core.shared.Paths;
import com.subhadip.datory.preprocessing.model.PayloadModel;
import com.subhadip.datory.preprocessing.pipeline.StageInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants.*;

public class StageFileProcessing implements StageInterface<PayloadModel, PayloadModel>, Serializable {
    private static final long serialVersionUID = 7531650076368430439L;
    private static final Logger LOG = LoggerFactory.getLogger(StageApplicationInitialize.class);
    private final String STAGE_NAME = this.getClass().getCanonicalName();

    @Override
    public PayloadModel run(PayloadModel payload) {
        LOG.info("Starting State 3: File Processing");

        new Parameters().load(payload);
        new Paths().load(payload);

        try {
            if(LAYOUT_CODE_DELIMITED.equals(payload.getLayoutModel().getLayoutCd())){
                new DelimitedFileAttributes().load(payload);
                new DelimitedFileDriver().execute(payload);
            }
            payload.getStatusModel().addToList(payload.getStatusModel().getStageLogs(), STAGE_NAME, STAGE_SUCCEEDED);
        } catch (ApplicationException e) {
            LOG.error(Throwables.getStackTraceAsString(e));
            payload.getStatusModel().setStatus(STAGE_FAILED);
            payload.getStatusModel().setIsFinalStatus(_YES_);
            payload.getStatusModel().addToList(payload.getStatusModel().getStageLogs(), STAGE_NAME, Throwables.getStackTraceAsString(e));
        }

        return payload;
    }

}
