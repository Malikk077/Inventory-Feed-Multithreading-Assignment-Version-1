package com.litmus7.inventoryfeedmultithreading.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class ErrorMessageUtil {

    private static Properties properties = new Properties();

    static {
        try (InputStream input = ErrorMessageUtil.class.getClassLoader()
                .getResourceAsStream("errorcode.properties")) {

            if (input != null) {
                properties.load(input);
            } else {
                System.err.println("errorcode.properties file not found in classpath!");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getErrorMessage(String errorCode) {
        return properties.getProperty(errorCode, "Unknown error code: " + errorCode);
    }
}
