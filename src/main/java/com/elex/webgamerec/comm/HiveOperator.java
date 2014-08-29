package com.elex.webgamerec.comm;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



public class HiveOperator {
	private static Log log = LogFactory.getLog(HiveOperator.class);	
	private static Connection conn;
	public static Connection getHiveConnection() {
		if(conn == null){
			try {
				Class.forName("org.apache.hive.jdbc.HiveDriver");
			} catch (ClassNotFoundException e) {
				log.error(new Date()+":Driver class can not found!");
				System.exit(1);
			}						
			try {
				conn = DriverManager.getConnection(PropertiesUtils.getHiveurl(),PropertiesUtils.getHiveUser(),"");
			} catch (SQLException e) {
				log.error(new Date()+":hive connection init error!["+PropertiesUtils.getHiveurl()+"]");
			}
		}		
		return conn;
	}
	
	public static int loadDataToHiveTable(String hql) throws SQLException{
		int result = 1;
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		Boolean t = stmt.execute(hql);
		stmt.close();
		if(t){
			return 0;
		}else{
			return result;
		}		
	}
	
	
	public static boolean executeHQL(String hql) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		stmt.execute(hql);
		stmt.close();
		return true;
	}
	
	public static List<Map<String,Object>> executeQueryHQL(String hql) throws SQLException{
		Connection con = HiveOperator.getHiveConnection();
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery(hql);
		List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
		ResultSetMetaData md = rs.getMetaData();
		int columnCount = md.getColumnCount();		
		while(rs.next()){
			Map<String,Object> rowData = new HashMap<String,Object>();
			for (int i = 1; i <= columnCount; i++) {
				rowData.put(md.getColumnName(i), rs.getObject(i));
				}
			result.add(rowData);			
		}
		stmt.close();
		return result;
	}

}
