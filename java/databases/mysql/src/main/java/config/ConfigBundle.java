package config;

import java.util.ResourceBundle;

/**
 * Project: mysql
 * Contributed By: Tushar Mudgal
 * On: 2019-07-01 | 20:39
 */
public class ConfigBundle {
    private static ResourceBundle resourceBundle ;

    /**
     * A private function that returns ResourceBundle instance
     * @return ResourceBundle
     */
    private static ResourceBundle getInstance(){
        if (resourceBundle == null) {
            resourceBundle = ResourceBundle.getBundle("config");
        }
        return resourceBundle;
    }

    /**
     *
     * @return config value based on key
     */
    public static String getValue(String key) {
        return ConfigBundle.getInstance().getString(key);
    }
}
