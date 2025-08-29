package com.litmus7.inventoryfeedmultithreading.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.litmus7.inventoryfeedmultithreading.property.DbConfig;



public class DBUtil{
	
    public static Connection getConnection() throws SQLException {
    	DbConfig config = DbConfig.getDatabaseConfig();

        return DriverManager.getConnection(config.getUrl(),config.getUsername(),config.getPassword());
    }
}
