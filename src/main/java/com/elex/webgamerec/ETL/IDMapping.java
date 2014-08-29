package com.elex.webgamerec.ETL;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import com.elex.webgamerec.comm.Constants;
import com.elex.webgamerec.comm.HdfsUtils;
import com.elex.webgamerec.comm.PropertiesUtils;

public class IDMapping {	

	private static Map<String,Integer> uidStrIntMap;
	private static Map<String,Integer> gidStrIntMap;
	private static Map<Integer,String> uidIntStrMap;
	private static Map<Integer,String> gidIntStrMap;
	private static Path uidMappingFile = new Path(PropertiesUtils.getRootDir()+Constants.UIDMAPPINGFILE);
	private static Path gidMappingFile = new Path(PropertiesUtils.getRootDir()+Constants.GIDMAPPINGFILE);

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		createIdMappingFile();
	}
	
	public static Map<String,Integer> getUidStrIntMap() throws IOException{
		if(uidStrIntMap==null){
			Configuration conf = new Configuration();
		    FileSystem fs = FileSystem.get(conf);		      
			uidStrIntMap = IDMapping.readIdMapFile(fs,uidMappingFile);
		}
		return uidStrIntMap;
	}
	
	public static Map<String,Integer> getGidStrIntMap() throws IOException{
		if(gidStrIntMap==null){
			Configuration conf = new Configuration();
		    FileSystem fs = FileSystem.get(conf);		    
			gidStrIntMap = IDMapping.readIdMapFile(fs,gidMappingFile);
		}
		return gidStrIntMap;
	}
	
	public static Map<Integer,String> getUidIntStrMap() throws IOException{
		if(uidIntStrMap==null){
			Configuration conf = new Configuration();
		    FileSystem fs = FileSystem.get(conf);
			uidIntStrMap = IDMapping.readIntStrIdMapFile(fs, uidMappingFile);
		}
		return uidIntStrMap;
	}
	
	public static Map<Integer,String> getGidIntStrMap() throws IOException{
		if(gidIntStrMap==null){
			Configuration conf = new Configuration();
		    FileSystem fs = FileSystem.get(conf);
		    gidIntStrMap = IDMapping.readIntStrIdMapFile(fs, gidMappingFile);
		}
		return gidIntStrMap;
	}

	
	

	public static int createIdMappingFile() throws IOException{
		String uri = PropertiesUtils.getRootDir()+Constants.STANDARDIZE;
		String uid = PropertiesUtils.getRootDir()+Constants.UIDMAPPINGFILE;
		String gid = PropertiesUtils.getRootDir()+Constants.GIDMAPPINGFILE;
		
		return createIdMappingFile(uri,uid,gid);
	}
	
	public static int createIdMappingFile(String uri,String uid,String gid) throws IOException{
		Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] files = fs.listStatus(new Path(uri));
        Path hdfs_src;
        BufferedReader reader = null;
        Set<String> uidSet = new HashSet<String>();
        Set<String> gidSet = new HashSet<String>();		
		
        for(FileStatus file:files){
        	
        	if(!file.isDirectory()){
        		hdfs_src = file.getPath();
        		if(file.getPath().getName().contains("part")){
        			try {
        	            reader = new BufferedReader(new InputStreamReader(fs.open(hdfs_src)));      	                    	            
        	            String line =reader.readLine();
        	            while(line != null){
        	            	String[] vList = line.split(",");
        	            	uidSet.add(vList[0]);
        	            	gidSet.add(vList[1]);
        	            	line = reader.readLine();
        	            }
        	           reader.close();
        	        } finally {
        	            IOUtils.closeStream(reader);
        	        }
        			
        		}
        	}
        } 
        
        Path uidMappingFile = new Path(uid);
        HdfsUtils.delFile(fs, uidMappingFile.toString());
        writeSetToFile(fs,uidSet,uidMappingFile);
        
        Path gidMappingFile = new Path(gid);
        HdfsUtils.delFile(fs, gidMappingFile.toString());
        writeSetToFile(fs,gidSet,gidMappingFile);
		
		return 0;		
	}
	
	public static void writeSetToFile(FileSystem fs, Set<String> set,Path dest) throws IOException{
		FSDataOutputStream out = fs.create(dest);
		Iterator<String> ite = set.iterator();
		int i = 1;
		while(ite.hasNext()){
			out.write(Bytes.toBytes(new String(i+","+ite.next()+"\r\n")));
			i++;
		}		
		out.close();		
	}
	
	public static Map<String,Integer> readIdMapFile(FileSystem fs,Path src) throws IOException{
		Map<String,Integer> idMap = new HashMap<String,Integer>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(src))); 
		String line =reader.readLine();
        while(line != null){
        	String[] vList = line.split(",");
        	if(vList.length==2){
        		idMap.put(vList[1],Integer.parseInt(vList[0]));
        	}
        	
        	line = reader.readLine();
        }
        reader.close();
		return idMap;
		
	}
	
	
	public static Map<Integer,String> readIntStrIdMapFile(FileSystem fs,Path src) throws IOException{
		Map<Integer,String> idMap = new HashMap<Integer,String>();
		BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(src))); 
		String line =reader.readLine();
        while(line != null){
        	String[] vList = line.split(",");
        	if(vList.length==2){
        		idMap.put(Integer.parseInt(vList[0]),vList[1]);
        	}
        	
        	line = reader.readLine();
        }
        reader.close();
		return idMap;
		
	}
}
