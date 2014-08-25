package com.elex.webgamerec.comm;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class GameListUtils {

	private Set<String> miniGame;
	private Set<String> webGame;
	private Map<String,String> gameTagMap;
	
	public GameListUtils() throws IOException {
		miniGame = new HashSet<String>();
		webGame = new HashSet<String>();
		gameTagMap = new HashMap<String,String>();
		getGameTypeAndTag();
	}

	
	
	public Set<String> getMiniGame() {
		return miniGame;
	}

	public Set<String> getWebGame() {
		return webGame;
	}

	public Map<String, String> getGameTagMap() {
		return gameTagMap;
	}

	
	private void getGameTypeAndTag() throws IOException{
		Configuration configuration = HBaseConfiguration.create();
		HTable gm = new HTable(configuration, "gm_gidlist");
		gm.setAutoFlush(false);
		Scan s = new Scan();
		s.setCaching(500);
		s.addColumn(Bytes.toBytes("gm"), Bytes.toBytes("gt"));
		s.addColumn(Bytes.toBytes("gm"), Bytes.toBytes("pt"));
		ResultScanner rs = gm.getScanner(s);
		for (Result r : rs) {
			if(!r.isEmpty()){
				if(r.containsColumn(Bytes.toBytes("gm"), Bytes.toBytes("gt"))){
					if(r.getColumnLatest(Bytes.toBytes("gm"), Bytes.toBytes("gt")).getValue() != null){
						if(Bytes.toString(r.getColumnLatest(Bytes.toBytes("gm"), Bytes.toBytes("gt")).getValue()).equals("m")){
							miniGame.add(Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length-1)));
						}else if(Bytes.toString(r.getColumnLatest(Bytes.toBytes("gm"), Bytes.toBytes("gt")).getValue()).equals("w")){
							webGame.add(Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length-1)));
						}
					}					
				}
				
				if(r.containsColumn(Bytes.toBytes("gm"), Bytes.toBytes("pt"))){
					if(r.getColumnLatest(Bytes.toBytes("gm"), Bytes.toBytes("pt")).getValue() != null){
						gameTagMap.put(Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length-1)), Bytes.toString(r.getColumnLatest(Bytes.toBytes("gm"), Bytes.toBytes("pt")).getValue()));
					}					
				}
			}
		}
		
		gm.close();		
		
	}
	
}
