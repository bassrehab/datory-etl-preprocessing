package com.subhadip.datory.preprocessing.utils;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.sql.Clob;
import java.sql.SQLException;

public class StringUtils {

    public String castClobAsString(Clob clobObj) throws IOException, SQLException {
        InputStream in = clobObj.getAsciiStream();
        StringWriter writer = new StringWriter();
        IOUtils.copy(in, writer);
        return writer.toString();
    }
}
