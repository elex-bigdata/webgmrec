package com.elex.webgamerec.ETL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.elex.webgamerec.comm.Constants;
import com.elex.webgamerec.comm.GameListUtils;
import com.elex.webgamerec.comm.HdfsUtils;
import com.elex.webgamerec.comm.PropertiesUtils;

public class InputCollector extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new InputCollector(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"input-collector");
		job.setJarByClass(InputCollector.class);
		long now = System.currentTimeMillis();
	    long before = now - Long.valueOf(PropertiesUtils.getMergeDays()*24L*60L*60L*1000L);
 
	    List<Scan> scans = new ArrayList<Scan>(); 
	    Scan hbScan = new Scan();
		hbScan.setStartRow(Bytes.add(Bytes.toBytes("hb"), Bytes.toBytes(before)));
		hbScan.setStopRow(Bytes.add(Bytes.toBytes("hb"), Bytes.toBytes(now)));
		hbScan.setCaching(500);
		hbScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("gm_user_action"));
		hbScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("gt"));
		hbScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("cl"));
		scans.add(hbScan);
		
		TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class,Text.class, Text.class, job);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		String output = PropertiesUtils.getRootDir()+Constants.INPUTDIR;
		HdfsUtils.delFile(fs, output);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true)?0:1;
	}
		
	
	public static class MyMapper extends TableMapper<Text, IntWritable> {
		private String gid,uid,lang,gmType,tags;
		private String[] ugid,tagList;
		private Map<String,String> gameTagMap;
		private IntWritable one = new IntWritable(1);
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			GameListUtils hbaseOpe = new GameListUtils();
			gameTagMap = hbaseOpe.getGameTagMap();
		}
		
		
		@Override
		protected void map(ImmutableBytesWritable key, Result r,
				Context context) throws IOException, InterruptedException {
			if (!r.isEmpty()) {
				ugid = Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length-10)).split("\u0001");
				if(ugid.length==2){
					uid = ugid[1];
					gid = ugid[0];
				}
				
				for (KeyValue kv : r.raw()) {										
					if ("ua".equals(Bytes.toString(kv.getFamily()))&& "gt".equals(Bytes.toString(kv.getQualifier()))) {
						gmType = Bytes.toString(kv.getValue());
					}
					if ("ua".equals(Bytes.toString(kv.getFamily()))&& "cl".equals(Bytes.toString(kv.getQualifier()))) {
						lang = Bytes.toString(kv.getValue());
					}
				}
				
				gmType=gmType==null?null:gmType.substring(0, 1);
				lang = lang==null?"pt":lang;
				
				if(gmType != null && uid !=null && gid != null){
					if(gmType.equals("m")){
						tags = gameTagMap.get(gid);
						if(tags!=null){
							tagList = tags.split(":");
							for(String tag:tagList){
								context.write(new Text(uid+","+tag+","+gmType+","+lang), one);
							}
						}
					}else if(gmType.equals("w")){
						context.write(new Text(uid+","+gid+","+gmType+","+lang), one);
					}
				}
			}
											
		}
					
	}
	
	public static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
		private int count;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			
			count = 0;
			
			for(IntWritable v:values){				
				count = count+v.get();				
			}
			context.write(key, new IntWritable(count));
			
		}	
	}
	public static class MyReducer extends Reducer<Text, IntWritable, Text, Text> {

		private int sum;
		private String uid,gid,gmType,lang;
		private String[] vList;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			sum = 0;
			vList = key.toString().split(",");
			if(vList != null){
				if(vList.length==4){
					uid=vList[0];
					gid=vList[1];
					gmType=vList[2];
					lang=vList[3];
				}
			}
			
			for(IntWritable v:values){
				sum = sum + v.get();
			}
			
			context.write(null, new Text(uid+","+gid+","+sum+","+gmType+","+lang));
			
		}		
				
	}

}
