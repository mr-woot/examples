package util;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * Project: mysql
 * Contributed By: Tushar Mudgal
 * On: 2019-07-01 | 20:49
 */
public interface MySQLInterface {
    /**
     * Returns list of all databases.
     * @return List<String>
     */
    List<String> getCatalogs() throws SQLException, IOException, PropertyVetoException;

    /**
     * Returns list of all tables in a database.
     * @return List<String>
     */
    List<String> getTables() throws SQLException, IOException, PropertyVetoException;

    /**
     * Returns list of all tables in a database.
     * @return List<String>
     */
    List<String> getTablesForDatabase(String catalog) throws SQLException, IOException, PropertyVetoException;

    List<String> getTablesForDatabaseWithPattern(String catalog, String pattern) throws SQLException, IOException, PropertyVetoException;

    /**
     * Returns list of schemas in the form of json objects.
     * @return List<List<JSONObject>>
     */
    JSONObject getSchemas() throws SQLException, IOException, PropertyVetoException, JSONException;

    /**
     * Returns schema of a specific table.
     * @return List<JSONObject>
     */
    JSONObject getSchemaByDatabaseAndTable(String databaseName, String tableName) throws SQLException, IOException, PropertyVetoException, JSONException;

}
