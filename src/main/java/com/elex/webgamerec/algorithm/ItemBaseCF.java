package com.elex.webgamerec.algorithm;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

import com.elex.webgamerec.ETL.FilterUtils;
import com.elex.webgamerec.ETL.IDMapping;
import com.elex.webgamerec.comm.Constants;
import com.elex.webgamerec.comm.HdfsUtils;
import com.elex.webgamerec.comm.PropertiesUtils;
import com.elex.webgamerec.comm.StrLineParseTool;

public class ItemBaseCF implements StrLineParseTool{

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		

	}
	
	public static int RunItemCf() throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FilterUtils.writeFilerFile(fs, FilterUtils.getMiniGM(), new Path(PropertiesUtils.getRootDir()+Constants.FILTERFILE));
		String cfOut = PropertiesUtils.getRootDir()+Constants.CFOUT;
		String cfTemp = PropertiesUtils.getRootDir()+Constants.CFTEMP;
		HdfsUtils.delFile(fs, cfOut);
		HdfsUtils.delFile(fs, cfTemp);
		List<String> argList = new ArrayList<String>();
		argList.add("--input");
		argList.add(PropertiesUtils.getRootDir()+Constants.CFINPUT);
		argList.add("--output");
		argList.add(cfOut);
		argList.add("--numRecommendations");
		argList.add(PropertiesUtils.getCfNumOfRec());
		argList.add("--similarityClassname");
		argList.add(PropertiesUtils.getCfSimilarityClassname());
		argList.add("--tempDir");
		argList.add(cfTemp);
		argList.add("--itemsFile");
		argList.add(PropertiesUtils.getRootDir()+Constants.FILTERFILE);
		
		String[] args = new String[argList.size()];
		argList.toArray(args);
		
		return ToolRunner.run(new Configuration(), new RecommenderJob(), args);
	}
	
	public static int recParse() throws Exception{
		String[] args = new String[]{PropertiesUtils.getRootDir()+Constants.CFOUT,PropertiesUtils.getRootDir()+Constants.CFRECPARSE};
		return ToolRunner.run(new Configuration(), new CfRecParse(),args);		 
	}
	
		
	

	@Override
	public String parse(String line) throws Exception {		
		String[] uids = IDMapping.getUids();
		String[] gids = IDMapping.getGids();
	    
	    String[] kv = line.toString().split("\\s");
	    String uid = uids[Integer.parseInt(kv[0].trim())];
		String itemStr = kv[1].trim().replace("[", "").replace("]", "");
		String[] itemArr = itemStr.split(",");
		StringBuffer sb = new StringBuffer(200);
		sb.append(uid+"\t");
		for (int i = 0; i < itemArr.length; i++) {
			String[] item = itemArr[i].split(":");
			String gid = gids[Integer.parseInt(item[0])];
			sb.append(gid+":"+item[1]);
			if(i!=itemArr.length-1){
				sb.append(",");
			}
			
		}
		sb.append("\r\n");
		return sb.toString();
	}

}
