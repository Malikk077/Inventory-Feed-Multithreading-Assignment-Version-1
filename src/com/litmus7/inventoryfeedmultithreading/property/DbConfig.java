package com.litmus7.inventoryfeedmultithreading.property;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class DbConfig {
    private String url;
    private String username;
    private String password;

    public DbConfig(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public static DbConfig getDatabaseConfig() {
        Properties prop = new Properties();

        try (InputStream input = DbConfig.class.getClassLoader().getResourceAsStream("db.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find db.properties");
                return null;
            }

            prop.load(input);
            String url = prop.getProperty("db.url");
            String username = prop.getProperty("db.username");
            String password = prop.getProperty("db.password");
            
            if (url == null || username == null || password == null) {
                System.out.println("Missing required DB configuration.");
                return null;
            }

            return new DbConfig(url, username, password);

        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
    }

    public String getUrl() {
        return url;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
