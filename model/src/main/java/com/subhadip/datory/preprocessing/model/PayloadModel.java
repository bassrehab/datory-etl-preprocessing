package com.subhadip.datory.preprocessing.model;

import java.io.Serializable;

// Wrapper Model / Collection used in the overall pipeline

public class PayloadModel implements Serializable {

    private static final long serialVersionUID = 3240977841610575457L;

    private SourceModel sourceModel;
    private DestinationModel destinationModel;
    private LayoutModel layoutModel;
    private ParamsModel paramsModel;
    private StatusModel statusModel;

    public PayloadModel() {
        this.sourceModel = new SourceModel();
        this.destinationModel = new DestinationModel();
        this.layoutModel = new LayoutModel();
        this.paramsModel = new ParamsModel();
        this.statusModel = new StatusModel();
    }

    public ParamsModel getParamsModel() {
        return paramsModel;
    }

    public void setParamsModel(ParamsModel paramsModel) {
        this.paramsModel = paramsModel;
    }

    public SourceModel getSourceModel() {
        return sourceModel;
    }

    public void setSourceModel(SourceModel sourceModel) {
        this.sourceModel = sourceModel;
    }

    public DestinationModel getDestinationModel() {
        return destinationModel;
    }

    public void setDestinationModel(DestinationModel destinationModel) {
        this.destinationModel = destinationModel;
    }

    public LayoutModel getLayoutModel() {
        return layoutModel;
    }

    public void setLayoutModel(LayoutModel layoutModel) {
        this.layoutModel = layoutModel;
    }

    public StatusModel getStatusModel() {
        return statusModel;
    }

    public void setStatusModel(StatusModel statusModel) {
        this.statusModel = statusModel;
    }


}

