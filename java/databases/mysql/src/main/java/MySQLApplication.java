import lombok.extern.log4j.Log4j;
import org.codehaus.jettison.json.JSONException;
import util.impl.MySQLUtils;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Project: mysql
 * Contributed By: Tushar Mudgal
 * On: 2019-07-01 | 19:36
 */
@Log4j
public class MySQLApplication {
    public static void main(String[] args) {
        MySQLUtils mySQLUtils = new MySQLUtils();
        try {
            log.info(mySQLUtils.getSchemaByDatabaseAndTable("test", "services"));
        } catch (SQLException | IOException | PropertyVetoException | JSONException e) {
            e.printStackTrace();
        }
    }
}
