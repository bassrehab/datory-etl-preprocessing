package com.subhadip.datory.preprocessing.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class PropertyFileLoader {

    private static final Logger LOG = Logger.getLogger(PropertyFileLoader.class);

    public static Map<String, String> loadPropertiesFromFile(String propertyFile) {

        LOG.info(String.format("Reading from property file %s", propertyFile));
        Map<String, String> propertiesMap = new HashMap<>();
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            if (fs.exists(new Path(propertyFile))) {
                BufferedReader fileReader = new BufferedReader(new InputStreamReader(fs.open(new Path(propertyFile))));
                String line = null;
                while ((line = fileReader.readLine()) != null) {
                    String[] properties = line.split("=", 2);
                    if (properties.length == 2) {
                        propertiesMap.put(properties[0], properties[1]);
                    }
                }
                fileReader.close();
            }
        } catch (IOException ie) {
            LOG.error(String.format("Exception while reading from the property file %s.", propertyFile));
            throw new RuntimeException(ie);
        }
        return propertiesMap;
    }

}
