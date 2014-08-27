package com.elex.webgamerec.ETL;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.elex.webgamerec.comm.Constants;
import com.elex.webgamerec.comm.HiveOperator;
import com.elex.webgamerec.comm.PropertiesUtils;

public class DataAnalyzeUtils {

	public static boolean dataAnalyze() throws SQLException{
		
		boolean loadResult = false;
		boolean anaResult = false;
		String loadHql = "LOAD DATA INPATH '"+PropertiesUtils.getRootDir()+Constants.MERGE+"/part*' OVERWRITE INTO TABLE webgmrec_input";		
		loadResult = HiveOperator.executeHQL(loadHql);
		
		String hql = "INSERT OVERWRITE DIRECTORY '"+PropertiesUtils.getRootDir()+Constants.STANDARDIZE+"' " +
				"row format delimited fields terminated by ',' stored as textfile " +
				"select tgid,gt,lang,sum(hb_sum),count(distinct uid),max(hb_sum),min(hb_sum),avg(hb_sum),percentile(hb_sum,0.75) " +
				"from webgmrec_input group by tgid,gt,lang";
		
		anaResult = HiveOperator.executeHQL(hql);
				
		return loadResult && anaResult;
	}
	
	public static Map<String,DataAnalyzeDto> getDataAnalyzeResult() throws IOException{
		
		Map<String,DataAnalyzeDto> result = new HashMap<String,DataAnalyzeDto>();
		String uri = PropertiesUtils.getRootDir()+Constants.STANDARDIZE;
		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(new Path(uri));
        Path hdfs_src;
        BufferedReader reader = null;	
        
        String mixId,gid,gt,lang;
    	int sum,count,max,min;
    	Double avg, percentile;
    	
		
        for(FileStatus file:files){
        	
        	if(!file.isDirectory()){
        		hdfs_src = file.getPath();
        		try {
    	            reader = new BufferedReader(new InputStreamReader(fs.open(hdfs_src)));      	                    	            
    	            String line =reader.readLine();
    	            while(line != null){
    	            	String[] vList = line.split(",");  
    	            	if(vList.length==9){
    	            		mixId = vList[0]+vList[1]+vList[2];
    	            		gid = vList[0];
    	            		gt = vList[1];
    	            		lang = vList[2];
    	            		sum = Integer.valueOf(vList[3]);
    	            		count = Integer.valueOf(vList[4]);
    	            		max = Integer.valueOf(vList[5]);
    	            		min = Integer.valueOf(vList[6]);
    	            		avg = Double.valueOf(vList[7]);
    	            		percentile = Double.valueOf(vList[8]);
    	            		result.put(mixId, new DataAnalyzeDto(gid,gt,lang,sum,count,max,min,avg,percentile));
    	            	}
    	            	line = reader.readLine();
    	            }
    	           reader.close();
    	        } finally {
    	            IOUtils.closeStream(reader);
    	        }
        	}
        } 
		return result;
	}

}
