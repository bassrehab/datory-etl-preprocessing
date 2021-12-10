package com.subhadip.datory.preprocessing.pipeline;

public interface PipelineInterface<T> {

    DatoryPipeline pipe(StageInterface stage) throws Exception;
    Object start(T payload) throws Exception;
}
