package util.impl;

import connection.DataSource;
import lombok.extern.log4j.Log4j;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import util.MySQLInterface;
import util.StringUtils;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Project: mysql
 * Contributed By: Tushar Mudgal
 * On: 2019-07-01 | 21:01
 */
@Log4j
public class MySQLUtils implements MySQLInterface {
    @Override
    public List<String> getCatalogs() throws SQLException, IOException, PropertyVetoException {
        Connection connection;
        ResultSet resultSet;
        List<String> catalogs = new ArrayList<>();
        connection = DataSource.getInstance().getConnection();
        DatabaseMetaData md = connection.getMetaData();
        log.debug("Fetching databases...");
        resultSet = md.getCatalogs();
        while (resultSet.next()) {
            String databaseName = resultSet.getString(1);
            if (StringUtils.isOneOfInternalDatabases(databaseName)) {
                catalogs.add(databaseName);
            } else {
                log.debug("Internal database - '" + databaseName + "' skipped");
            }
        }
        connection.close();
        resultSet.close();
        return catalogs;
    }

    @Override
    public List<String> getTables() throws SQLException, IOException, PropertyVetoException {
        Connection connection;
        ResultSet resultSet;
        List<String> tablesList = new ArrayList<>();
        connection = DataSource.getInstance().getConnection();
        DatabaseMetaData md = connection.getMetaData();
        log.debug("Fetching databases...");
        resultSet = md.getCatalogs();
        while (resultSet.next()) {
            String databaseName = resultSet.getString(1);
            if (StringUtils.isOneOfInternalDatabases(databaseName)) {
                log.debug("Internal database - '" + databaseName + "' skipped");
            } else {
                ResultSet tablesResultSet;
                DatabaseMetaData tablesMd = connection.getMetaData();
                log.debug("Fetching tables for database - '" + databaseName + "'");
                tablesResultSet = tablesMd.getTables(databaseName, "%", "%", null);
                while (tablesResultSet.next()) {
                    tablesList.add(databaseName + "." + tablesResultSet.getString(3));
                }
                tablesResultSet.close();
            }
        }
        connection.close();
        resultSet.close();
        return tablesList;
    }

    @Override
    public List<String> getTablesForDatabaseWithPattern(String database, String pattern) throws SQLException, IOException, PropertyVetoException {
        Connection connection;
        ResultSet resultSet;
        List<String> tablesList = new ArrayList<>();
        connection = DataSource.getInstance().getConnection();
//        connection.setCatalog(database);
        DatabaseMetaData md = connection.getMetaData();
        log.debug("Fetching tables for database - '" + database + "'");
        resultSet = md.getTables(database, "%", pattern + "%", new String[]{"TABLE", "VIEW"});
        while (resultSet.next()) {
            tablesList.add(resultSet.getString(3));
        }
        connection.close();
        resultSet.close();
        return tablesList;
    }

    @Override
    public List<String> getTablesForDatabase(String database) throws SQLException, IOException, PropertyVetoException {
        Connection connection;
        ResultSet resultSet;
        List<String> tablesList = new ArrayList<>();
        connection = DataSource.getInstance().getConnection();
        connection.setCatalog(database);
        DatabaseMetaData md = connection.getMetaData();
        log.debug("Fetching tables for database - '" + database + "'");
        resultSet = md.getTables(database, "%", "%", new String[]{"TABLE", "VIEW"});
        while (resultSet.next()) {
            tablesList.add(resultSet.getString(3));
        }
        connection.close();
        resultSet.close();
        return tablesList;
    }

    @Override
    public JSONObject getSchemas() throws SQLException, IOException, PropertyVetoException, JSONException {
        Connection connection;
        ResultSet resultSet;
        JSONObject schemasList = new JSONObject();
        connection = DataSource.getInstance().getConnection();
        DatabaseMetaData md = connection.getMetaData();
        log.debug("Fetching databases...");
        resultSet = md.getCatalogs();
        while (resultSet.next()) {
            JSONObject dbList = new JSONObject();
            String databaseName = resultSet.getString(1);
            if (StringUtils.isOneOfInternalDatabases(databaseName)) {
                log.debug("Internal database - '" + databaseName + "' skipped");
            } else {
                connection.setCatalog(databaseName);
                ResultSet tablesResultSet;
                DatabaseMetaData tablesMd = connection.getMetaData();
                log.debug("Fetching tables for database - '" + databaseName + "'");
                tablesResultSet = tablesMd.getTables(databaseName, "%", "%", null);
                buildSchemaForTable(databaseName, connection, schemasList, dbList, tablesResultSet);
            }
        }
        connection.close();
        resultSet.close();
        return schemasList;
    }

    @Override
    public JSONObject getSchemaByDatabaseAndTable(String databaseName, String tableName) throws SQLException, IOException, PropertyVetoException, JSONException {
        Connection connection;
        JSONObject schemasList = new JSONObject();
        connection = DataSource.getInstance().getConnection();
        connection.setCatalog(databaseName);
        JSONObject dbList = new JSONObject();
        if (StringUtils.isOneOfInternalDatabases(databaseName)) {
            log.debug("Internal database - '" + databaseName + "' skipped");
        } else {
            ResultSet tablesResultSet;
            DatabaseMetaData tablesMd = connection.getMetaData();
            log.debug("Fetching tables for database - '" + databaseName + "'");
            tablesResultSet = tablesMd.getTables(databaseName, "%", tableName, null);
            buildSchemaForTable(databaseName, connection, schemasList, dbList, tablesResultSet);
        }
        connection.close();
        return schemasList;
    }

    private void buildSchemaForTable(String databaseName, Connection connection, JSONObject schemasList, JSONObject dbList, ResultSet tablesResultSet) throws SQLException, JSONException {
        while (tablesResultSet.next()) {
            String table =  tablesResultSet.getString(3);
            Statement st = connection.createStatement();
            ResultSet rstable = st.executeQuery("SELECT * FROM " + table + " limit 1");
            ResultSetMetaData rsMetaData = rstable.getMetaData();
            List<JSONObject> list = new ArrayList<>();
            for(int i = 1;i <= rsMetaData.getColumnCount();i++) {
                String columnName = rsMetaData.getColumnName(i);
                String columnType = rsMetaData. getColumnTypeName(i);
                JSONObject schemaKeyValue = new JSONObject();
                schemaKeyValue.put("COLUMN_POSITION", i);
                schemaKeyValue.put("COLUMN_NAME", columnName);
                schemaKeyValue.put("COLUMN_TYPE", columnType);
                list.add(schemaKeyValue);
            }
            dbList.put(table, list);
            rstable.close();
            st.close();
        }
        tablesResultSet.close();
        schemasList.put(databaseName, dbList);
    }
}
