package com.subhadip.datory.preprocessing.core.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.subhadipmitra.datory.preprocessing.common.connection.MetadataConnection;
import com.subhadipmitra.datory.preprocessing.common.exceptions.ApplicationException;
import com.subhadip.datory.preprocessing.model.PayloadModel;
import com.subhadip.datory.preprocessing.utils.JSONUtils;
import org.apache.log4j.Logger;

import java.sql.*;

import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants.LAYOUT_CODE_DELIMITED;
import static com.subhadipmitra.datory.preprocessing.common.constants.ApplicationConstants.LAYOUT_CODE_FIXEDWIDTH;

public class PreProcessingDAO {

    private Connection connection;
    private static final Logger LOG = Logger.getLogger(PreProcessingDAO.class);

    public PreProcessingDAO(MetadataConnection connection) {
        this.connection = connection.getConnection();
    }

    public PayloadModel getCtryCtrlCharacterReplacement(PayloadModel payloadModel) throws SQLException {
        LOG.info(String.format("Retrieving Control Character Information for Proc Id = %s and Ctry Code = %s",
                payloadModel.getParamsModel().getProcId(), payloadModel.getParamsModel().getProcCtryCd()));

        String query = "SELECT CTRL_CHAR_PATTERN, REPLACEMENT_CHARS FROM DATORY_PROC_CTRY_CTRL_CHAR WHERE PROC_ID = ? AND CTRY_CD = ?";

        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, payloadModel.getParamsModel().getProcId());
            statement.setString(2, payloadModel.getParamsModel().getProcCtryCd());

            ResultSet rs = statement.executeQuery();
            while (rs.next()) payloadModel.getSourceModel().setControlCharReplacementMap(rs.getString("CTRL_CHAR_PATTERN"),
                    rs.getString("REPLACEMENT_CHARS"));
            rs.close();
        } catch (SQLException e) {
            LOG.error("Error while retrieving Control Character Information from DB");
            throw e;
        }
        return payloadModel;
    }

    public PayloadModel getDestinationDetails(PayloadModel payloadModel) throws SQLException {
        LOG.info(String.format("Retrieving Destination Details for Proc Id = %s", payloadModel.getParamsModel().getProcId()));

        String query = "SELECT STG_DIR_NM, STG_DB_NM, STG_TBL_NM, STG_TBL_PART_TXT FROM DATORY_LOAD_PROCESS WHERE PROC_ID = ?";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, payloadModel.getParamsModel().getProcId());

            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                payloadModel.getDestinationModel().setStagingDbName(rs.getString("STG_DB_NM"));
                payloadModel.getDestinationModel().setStagingDirName(rs.getString("STG_DIR_NM"));
                payloadModel.getDestinationModel().setStagingTableName(rs.getString("STG_TBL_NM"));
                payloadModel.getDestinationModel().setStagingTablePartitionText(rs.getString("STG_TBL_PART_TXT"));
            }
            rs.close();
        } catch (SQLException e) {
            LOG.error("Error while retrieving Destination Information from DB");
            throw e;
        }
        return payloadModel;
    }

    /**
     * Only applicable for delimited files?
     * @param payloadModel Processing Model
     * @return payloadModel
     * @throws SQLException
     */
    public PayloadModel getNumberOfColumns(PayloadModel payloadModel) throws SQLException, ApplicationException {
        LOG.info("Retrieving number of columns for fileId: " + payloadModel.getSourceModel().getFileId());

        if(LAYOUT_CODE_DELIMITED.equals(payloadModel.getLayoutModel().getLayoutCd())){
            String query = "SELECT max(FLD_NUM) AS COL_NUMS FROM DATORY_FIELD_DETAIL WHERE FILE_ID = ?";


            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setInt(1, payloadModel.getSourceModel().getFileId());

                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    payloadModel.getLayoutModel().getDelimited().setNumberOfColumns(rs.getInt("COL_NUMS"));
                }
                rs.close();
            } catch (SQLException e) {
                LOG.error("Error while retrieving number of columns");
                throw e;
            }
            return payloadModel;
        }
        else throw new ApplicationException("Trying to get Number of Columns for Non DELIMITED File Layout");

    }


    /**
     * Only for FIXED width files.
     * @param payloadModel Processing Model
     * @return
     * @throws SQLException
     */
    public PayloadModel getTotalRowLength(PayloadModel payloadModel) throws SQLException, ApplicationException {
        LOG.info("Retrieving total row length for fileId: " + payloadModel.getSourceModel().getFileId());

        if(LAYOUT_CODE_FIXEDWIDTH.equals(payloadModel.getLayoutModel().getLayoutCd())){
            String query = "SELECT SUM(FLD_LEN_NUM) AS TOTAL_LENGTH FROM DATORY_FIELD_DETAIL WHERE FILE_ID = ?";

            try (PreparedStatement statement = connection.prepareStatement(query)) {
                statement.setInt(1, payloadModel.getSourceModel().getFileId());

                ResultSet rs = statement.executeQuery();
                while (rs.next()) {
                    payloadModel.getLayoutModel().getFixedWidth().setRowSize(rs.getInt("TOTAL_LENGTH"));
                }
                rs.close();
            } catch (SQLException e) {
                LOG.error("Error while retrieving total row size");
                throw e;
            }
            return payloadModel;
        }
        else throw new ApplicationException("Trying to get Total Row Length for Non FIXED WIDTH File Layout");



    }


    public PayloadModel getFileDetails(PayloadModel payloadModel) throws SQLException {
        LOG.info(String.format("Retrieving File Details for Proc Id = %s", payloadModel.getParamsModel().getProcId()));

        String query = "SELECT FILE_ID, DIR_NM, FILE_NM, FILE_LAYOUT_CD, FILE_COL_DELIM_TXT, HDR_LINE_NUM, " +
                "FTR_LINE_NUM, ACHV_DIR_NM FROM DATORY_FILE_DETAIL WHERE PROC_ID = ?";


        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setString(1, payloadModel.getParamsModel().getProcId());

            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                payloadModel.getSourceModel().setFileId(rs.getInt("FILE_ID"));
                payloadModel.getSourceModel().setLandingFileName(rs.getString("FILE_NM"));
                payloadModel.getSourceModel().setLandingFolder(rs.getString("DIR_NM"));
                payloadModel.getSourceModel().setArchiveFolder(rs.getString("ACHV_DIR_NM"));


                String layoutCd = rs.getString("FILE_LAYOUT_CD");

                payloadModel.getLayoutModel().setLayoutCd(rs.getString("FILE_LAYOUT_CD"));

                if(LAYOUT_CODE_DELIMITED.equals(layoutCd)) // Do ONLY for Delimited Layout Code.
                    payloadModel.getLayoutModel().getDelimited().setColDelimiter(rs.getString("FILE_COL_DELIM_TXT"));

            }
            rs.close();
        } catch (SQLException e) {
            LOG.error("Error while retrieving Control Character Information from DB");
            throw e;
        }
        return payloadModel;

    }

    public void flushLogsToDb(PayloadModel payloadModel) throws SQLException, JsonProcessingException {

            LOG.info("Flushing Stage Details to DB");
            String checkQuery = "SELECT COUNT('PROC_INSTANCE_ID') AS rowExist FROM DATORY_PREPROCESSING_STATUS where PROC_INSTANCE_ID = ?";
            String insertQuery = "INSERT INTO DATORY_PREPROCESSING_STATUS " +
                    "(PROC_ID, PROC_INSTANCE_ID, BIZ_DT, CTRY_CD, STATUS, LOGS, IS_FINAL_STATUS) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?)";

            String updateQuery = "UPDATE DATORY_PREPROCESSING_STATUS SET " +
                    "STATUS = ?, LOGS = ?, IS_FINAL_STATUS = ? WHERE  PROC_INSTANCE_ID = ?";

        try (PreparedStatement statement = connection.prepareStatement(checkQuery)) {
            statement.setString(1, payloadModel.getParamsModel().getProcInstanceId());
            ResultSet rs = statement.executeQuery();

            JSONUtils jsonUtils = new JSONUtils();

            if(rs.getInt("rowExist") > 0){
                // Update
                try (PreparedStatement stmtUpdate = connection.prepareStatement(updateQuery)) {
                    stmtUpdate.setString(1, payloadModel.getStatusModel().getStatus());
                    stmtUpdate.setClob(2, new javax.sql.rowset.serial.SerialClob(jsonUtils
                            .HMapListToJSON(payloadModel.getStatusModel().getStageLogs()).toCharArray()));
                    stmtUpdate.setString(3, payloadModel.getStatusModel().getIsFinalStatus());
                    stmtUpdate.setString(4, payloadModel.getParamsModel().getProcInstanceId());

                    ResultSet rsUpdate = stmtUpdate.executeQuery();
                    rsUpdate.close();
                }
            }
            else{

                // Insert
                try (PreparedStatement stmtInsert = connection.prepareStatement(insertQuery)) {
                    stmtInsert.setString(1, payloadModel.getParamsModel().getProcId());
                    stmtInsert.setString(2, payloadModel.getParamsModel().getProcInstanceId());
                    stmtInsert.setString(3, payloadModel.getParamsModel().getProcDate());
                    stmtInsert.setString(4, payloadModel.getParamsModel().getProcCtryCd());
                    stmtInsert.setString(5, payloadModel.getStatusModel().getStatus());
                    stmtInsert.setClob(6, new javax.sql.rowset.serial.SerialClob(jsonUtils
                            .HMapListToJSON(payloadModel.getStatusModel().getStageLogs()).toCharArray()));
                    stmtInsert.setString(7, payloadModel.getStatusModel().getIsFinalStatus());

                    ResultSet rsInsert = stmtInsert.executeQuery();
                    rsInsert.close();

                }
            }
            rs.close();
        }
    }

}
