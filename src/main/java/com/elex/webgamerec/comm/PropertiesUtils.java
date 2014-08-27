package com.elex.webgamerec.comm;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


public class PropertiesUtils {

	private static Properties pop = new Properties();
	static{
		InputStream is = null;
		try{
			is = PropertiesUtils.class.getClassLoader().getResourceAsStream("config.properties");
			pop.load(is);
		}catch(Exception e){
			e.printStackTrace();
			
		}finally{
			try {
				if(is!=null)is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
		
	
	
	public static int getMergeDays(){
		return Integer.parseInt(pop.getProperty("mergeDays"));
	}
	
	public static String getRootDir(){
		return pop.getProperty("rootdir");
	}

	public static String getHiveurl() {
		return pop.getProperty("hive.url");
	}

}
