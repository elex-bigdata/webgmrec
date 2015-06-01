package com.elex.webgamerec.ETL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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

public class YacUA extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new YacUA(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"Yac-user-action-parse");
		job.setJarByClass(YacUA.class);
		long now = System.currentTimeMillis();
	    long before = now - Long.valueOf(Integer.parseInt(args[0])*24L*60L*60L*1000L);
 
	    List<Scan> scans = new ArrayList<Scan>(); 

	    Scan hbScan = new Scan();
		hbScan.setStartRow(Bytes.add(new byte[]{(byte) 1}, Bytes.toBytes(before)));
		hbScan.setStopRow(Bytes.add(new byte[]{(byte) 1}, Bytes.toBytes(now)));
		hbScan.setCaching(500);
		hbScan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes("yac_user_action"));
		hbScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("p"));
		hbScan.addColumn(Bytes.toBytes("ua"), Bytes.toBytes("nt"));
		scans.add(hbScan);
		
		TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class,Text.class, Text.class, job);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true)?0:1;
	}
		
	
	public static class MyMapper extends TableMapper<Text, IntWritable> {
		
			
		@Override
		protected void map(ImmutableBytesWritable key, Result r,
				Context context) throws IOException, InterruptedException {
			if (!r.isEmpty()) {
				String uid = Bytes.toString(Bytes.tail(r.getRow(), r.getRow().length-9));
				String pid = null;
				String nation = null;
				for (Cell kv : r.listCells()) {		
					if ("p".equals(Bytes.toString(CellUtil.cloneQualifier(kv)))) {
						pid = Bytes.toString(CellUtil.cloneValue(kv))==null?null:Bytes.toString(CellUtil.cloneValue(kv)).trim().toLowerCase();
					}
					if ("nt".equals(Bytes.toString(CellUtil.cloneQualifier(kv)))) {
						nation = Bytes.toString(CellUtil.cloneValue(kv))==null?null:Bytes.toString(CellUtil.cloneValue(kv)).trim().toLowerCase();
					}
				}
				
				context.write(new Text(uid+""+pid+","+nation), new IntWritable(1));
				
			}
											
		}
		
					
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, Text> {
		
		private int count;
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
							
			count = 0;
			
			for(IntWritable v:values){				
				count = count+v.get();				
			}
						
			context.write(null,new Text(key.toString()+","+count));
						
		}	
	}
}
