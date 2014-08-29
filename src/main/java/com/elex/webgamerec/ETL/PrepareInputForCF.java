package com.elex.webgamerec.ETL;

import java.util.Map;

import com.elex.webgamerec.comm.Constants;
import com.elex.webgamerec.comm.ParseUtils;
import com.elex.webgamerec.comm.PropertiesUtils;
import com.elex.webgamerec.comm.StrLineParseTool;

public class PrepareInputForCF  implements StrLineParseTool{

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		prepareInput();
	}
	
	
	public static int prepareInput() throws Exception{
		String input = PropertiesUtils.getRootDir()+Constants.STANDARDIZE;
		String output = PropertiesUtils.getRootDir()+Constants.CFINPUT;
		PrepareInputForCF tool = new PrepareInputForCF();
		prepareInput(input,output,tool);
		return 0;
	}
	
	public static void prepareInput(String input,String output,StrLineParseTool tool) throws Exception{
		ParseUtils.parseTextOutput(input, output, tool);
	}


	@Override
	public String parse(String line) throws Exception {
		Map<String,Integer> uidMap = IDMapping.getUidStrIntMap();
	    Map<String,Integer> gidMap = IDMapping.getGidStrIntMap();
		
		String[] vList = line.split(",");
		
    	if(vList.length==5){
        	return new String(Integer.toString(uidMap.get(vList[0]))+","+Integer.toString(gidMap.get(vList[1]))+","+vList[2]+"\r\n");
    	}
		return null;
	}

}
