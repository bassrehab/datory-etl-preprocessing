package com.subhadip.datory.preprocessing.core;

import com.google.common.base.Throwables;
import com.subhadipmitra.datory.preprocessing.common.exceptions.ApplicationException;
import com.subhadip.datory.preprocessing.core.stages.StageApplicationFinalize;
import com.subhadip.datory.preprocessing.core.stages.StageApplicationInitialize;
import com.subhadip.datory.preprocessing.core.stages.StageFetchMetadata;
import com.subhadip.datory.preprocessing.core.stages.StageFileProcessing;
import com.subhadip.datory.preprocessing.model.PayloadModel;
import com.subhadip.datory.preprocessing.pipeline.DatoryOperation;
import com.subhadip.datory.preprocessing.pipeline.DatoryPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws ApplicationException {
        Application app = new Application();
        PayloadModel payload = new PayloadModel();
        payload.getParamsModel().setArgsJSON(args[0]);

        // Define Stages
        StageApplicationInitialize appInit = new StageApplicationInitialize(); // Stage 1
        StageFetchMetadata appMetadata = new StageFetchMetadata(); // Stage 2
        StageFileProcessing appFileProcessing = new StageFileProcessing(); // Stage 3
        StageApplicationFinalize appFinalize = new StageApplicationFinalize(); // Stage 4

        // Define Pipeline
        DatoryPipeline<PayloadModel> p = new DatoryPipeline<>(new DatoryOperation<>());

        // Register Stages and Execute
        try {
            p = p.pipe(appInit)
                    .pipe(appMetadata)
                    .pipe(appFileProcessing)
                    .pipe(appFinalize);
        } catch (Exception e) {
            LOG.error(Throwables.getStackTraceAsString(e));
            throw new ApplicationException("Encountered exception in "+ app.getCurrentPackage(), e);
        }
        p.run(payload);

    }



    private String getCurrentPackage() {
        return this.getClass().getPackage().getName();
    }

}
