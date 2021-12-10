package com.subhadip.datory.preprocessing.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

public class JSONUtils implements Serializable {

    private static final long serialVersionUID = -1918203672088163L;

    public String HMapToJSON(HashMap<String, String> map) throws JsonProcessingException {
        return new ObjectMapper().writeValueAsString(map);
    }
    public String HMapListToJSON(HashMap<String, List<String>> map) throws JsonProcessingException {
        return new ObjectMapper().writeValueAsString(map);
    }
}
