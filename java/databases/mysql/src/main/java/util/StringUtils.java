package util;

import java.util.ArrayList;
import java.util.List;

/**
 * Project: mysql
 * Contributed By: Tushar Mudgal
 * On: 2019-07-01 | 22:19
 */
public class StringUtils {

    private static List<String> mySQLInternalTables = new ArrayList<>();

    static {
        mySQLInternalTables.add("information_schema");
        mySQLInternalTables.add("performance_schema");
        mySQLInternalTables.add("mysql");
        mySQLInternalTables.add("sys");
    }

    public static boolean isOneOfInternalDatabases(String database) {
        return mySQLInternalTables.contains(database);
    }
}
