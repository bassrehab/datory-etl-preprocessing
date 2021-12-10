package com.subhadipmitra.datory.preprocessing.common.connection;
import com.subhadipmitra.datory.preprocessing.common.config.GenericConfigurationManager;
import com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants;
import com.subhadipmitra.datory.preprocessing.common.exceptions.ApplicationException;
import com.subhadip.datory.preprocessing.utils.EncryptionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class MetadataConnection {

    private static final Logger LOG = Logger.getLogger(MetadataConnection.class);

    private String JDBCDriver;

    private String databaseConnectionString;

    private String databaseUserName;

    private String databasePassword;

    private Connection connection;

    private GenericConfigurationManager config;

    public MetadataConnection(GenericConfigurationManager genericConfigurationManager) {
        this.config = genericConfigurationManager;
        this.JDBCDriver = genericConfigurationManager.getApplicationProperties(ApplicationConstants.METADATA_DATABASE_DRIVER)
                .orElse(StringUtils.EMPTY);
        this.databaseConnectionString = genericConfigurationManager.getApplicationProperties(ApplicationConstants.METADATA_JDBC_CONNECTION_STRING)
                .orElse(StringUtils.EMPTY);
        this.databaseUserName = genericConfigurationManager.getApplicationProperties(ApplicationConstants.METADATA_DATABASE_USERNAME)
                .orElse(StringUtils.EMPTY);
        this.databasePassword = EncryptionUtil.decrypt(
                genericConfigurationManager.getApplicationProperties(ApplicationConstants.METADATA_DATABASE_PASSWORD).orElse(StringUtils.EMPTY));
        this.connection = initializeDatabaseConnection();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (this.connection != null) {
                try {
                    this.connection.close();
                } catch (Exception e) {
                    LOG.error("Error while closing database connection");
                    LOG.error(ExceptionUtils.getStackTrace(e));
                }
            }
        }));
    }

    private Connection initializeDatabaseConnection() {

        if (StringUtils.trimToNull(JDBCDriver) == null || StringUtils.trimToNull(databaseConnectionString) == null
                || StringUtils.trimToNull(databaseUserName) == null || StringUtils.trimToNull(databasePassword) == null) {
            throw new RuntimeException("Database Connection Properties are empty. Hence the application cannot be run");
        }

        try {
            Class.forName(JDBCDriver);
        } catch (ClassNotFoundException e) {
            LOG.error("Oracle JDBC Driver is not in classpath. Hence Exiting");
            throw new RuntimeException("Oracle JDBC Driver is not in classpath. Hence Exiting", e);
        }
        try {
            Connection connection = DriverManager.getConnection(databaseConnectionString, databaseUserName, databasePassword);
            return connection;
        } catch (SQLException e) {
            LOG.error("Error while connecting to oracle database!!!");
            throw new RuntimeException(e);
        }
    }

    public void closeDatabaseConnection() throws ApplicationException {
        if (this.connection != null) {
            try {
                this.connection.close();
            } catch (SQLException e) {
                LOG.error("Error while closing database connection");
                throw new ApplicationException(ExceptionUtils.getStackTrace(e), e );
            }
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public GenericConfigurationManager getConfig() {
        return config;
    }

    public void setConfig(GenericConfigurationManager config) {
        this.config = config;
    }
}