package com.subhadip.datory.preprocessing.pipeline;

import java.util.ArrayList;

public class DatoryOperation<T> implements OperationInterface<T> {

    public DatoryOperation() {
    }

    /**
     * Copy Constructor
     *
     * @param copy
     */
    public DatoryOperation(OperationInterface copy) {
    }

    public Object operate(ArrayList<StageInterface> stages, T payload) {
        ArrayList<Object> payloads = new ArrayList<>();
        payloads.add(payload);
        stages.forEach(stage -> payloads.add(stage.run(payloads.get(payloads.size() - 1))));

        return payloads.get(payloads.size() - 1);
    }
}