package com.subhadip.datory.preprocessing.pipeline;

import java.util.ArrayList;

public interface OperationInterface<T> {

    Object operate(ArrayList<StageInterface> stages, T payload) throws Exception;
}
