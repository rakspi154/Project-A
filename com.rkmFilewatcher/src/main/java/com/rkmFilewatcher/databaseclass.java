package com.rkmFilewatcher;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.io.InputStream;


public class databaseclass {
	
	
	
	public static Connection startConnection(String strURL, String strUserName, String strPassword)
	{

		
		Connection conn = null;
		String url = strURL;
		String dbName = "rkmdb";
		String driver = "org.mariadb.jdbc.Driver";
		String userName = strUserName;
		String password = strPassword;
		try {
			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(url,userName,password);
			
		} catch (Exception e) {
			System.out.println("NO CONNECTION");
		}
		
		return conn;
	}
	
	
	public static void closeConnection(Connection con)
	{
		try
		{
		    if(con != null)
		        con.close();
		    System.out.println("Connection closed !!");
		} catch (SQLException e) {
		    e.printStackTrace();
		}
	}
	
}
