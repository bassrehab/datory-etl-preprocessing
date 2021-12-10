package com.subhadipmitra.datory.preprocessing.common.config;

import com.subhadip.datory.preprocessing.utils.PropertyFileLoader;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


public class GenericConfigurationManager {

    private Map<String, String> applicationProperties = new HashMap<>();

    private Map<String, String> sparkProperties = new HashMap<>();

    public GenericConfigurationManager() {

    }

    public void loadApplicationProperties(String applicationPropertyFile) {
        applicationProperties.putAll(PropertyFileLoader.loadPropertiesFromFile(applicationPropertyFile));
    }

    public void loadSparkProperties(String sparkPropertyFile) {
        sparkProperties.putAll(PropertyFileLoader.loadPropertiesFromFile(sparkPropertyFile));
    }

    public Optional<String> getApplicationProperties(String key) {
        return Optional.ofNullable(applicationProperties.get(key));
    }

    public Map<String, String> getAllApplicationPropertiesWithPrefix(String prefix) {
        Map<String, String> applicationPropertiesWithPrefixMap = applicationProperties.entrySet().stream()
                .filter(f -> f.getKey().startsWith(prefix)).collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
        return applicationPropertiesWithPrefixMap;
    }

    public Map<String, String> getSparkProperties() {
        return Collections.unmodifiableMap(sparkProperties);
    }

}
