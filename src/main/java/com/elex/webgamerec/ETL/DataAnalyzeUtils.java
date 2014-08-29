package com.elex.webgamerec.ETL;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import com.elex.webgamerec.comm.Constants;
import com.elex.webgamerec.comm.HiveOperator;
import com.elex.webgamerec.comm.PropertiesUtils;

public class DataAnalyzeUtils {
	
	private static Map<String,DataAnalyzeDto> result;
	private static Map<String,List<DataAnalyzeDto>> webGameRank;
	

	public static void main(String[] args) throws Exception {
		analyze();
	}

	public static int analyze() throws SQLException{
		
		boolean loadResult = false;
		boolean anaResult = false;
		String loadHql = "LOAD DATA INPATH '"+PropertiesUtils.getRootDir()+Constants.MERGE+"/part*' OVERWRITE INTO TABLE webgmrec_input";		
		loadResult = HiveOperator.executeHQL(loadHql);
		
		String hql = "INSERT OVERWRITE TABLE "+Constants.HIVEANALYZETABLE+
				" select tgid,gt,lang,sum(hb_sum),count(distinct uid),max(hb_sum),min(hb_sum),avg(hb_sum),percentile(hb_sum,0.75),sqrt(var_pop(hb_sum)) " +
				"from webgmrec_input group by tgid,gt,lang";
		
		anaResult = HiveOperator.executeHQL(hql);
				
		return (loadResult && anaResult)?0:1;
	}
	
	
	public static Map<String,DataAnalyzeDto> getDataAnalyzeResult() throws IOException{
		if(result == null){
			result = readAnalyzeResult();
		}		
		return result;
	}
	
	public static Map<String,List<DataAnalyzeDto>> getRank() throws IOException{
		
		if(webGameRank == null){
			Map<String,List<DataAnalyzeDto>> rank = new HashMap<String,List<DataAnalyzeDto>>();
			Map<String,DataAnalyzeDto> anaResult = getDataAnalyzeResult();
			Collection<DataAnalyzeDto> collect = anaResult.values();
			Iterator<DataAnalyzeDto> ite = collect.iterator();
			while(ite.hasNext()){
				DataAnalyzeDto dto = ite.next();
				if(dto.getGt() != null){
					if(dto.getGt().equals("w")){
						
						if(rank.get(dto.getLanguage())==null){
							rank.put(dto.getLanguage(), new ArrayList<DataAnalyzeDto>());
						}
						rank.get(dto.getLanguage()).add(dto);
					}
				}
				
			}
			
			webGameRank=rank;
		}
				
		return webGameRank;
	}
	
	private static Map<String,DataAnalyzeDto> readAnalyzeResult() throws IOException{
		Map<String,DataAnalyzeDto> ana = new HashMap<String,DataAnalyzeDto>();		
		Set<String> webGameSet = new HashSet<String>();
		String uri = PropertiesUtils.getHiveWareHouse()+"/"+Constants.HIVEANALYZETABLE;
		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(new Path(uri));
        Path hdfs_src;
        BufferedReader reader = null;	
        
        String mixId,gid,gt,lang;
    	int sum,count,max,min;
    	Double avg, percentile,var;
    	
		
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
    	            		var = Double.valueOf(vList[9]);
    	            		result.put(mixId, new DataAnalyzeDto(gid,gt,lang,sum,count,max,min,avg,percentile,var));
    	            		if(gt.equals("w")){
    	            			webGameSet.add(gid);
    	            		}
    	            	}
    	            	line = reader.readLine();
    	            }
    	           reader.close();
    	        } finally {
    	            IOUtils.closeStream(reader);
    	        }
        	}
        }
        
        IDMapping.writeSetToFile(fs, webGameSet, new Path(PropertiesUtils.getRootDir()+Constants.FILTERFILE));
        
        return ana;
	}
	
	

}
