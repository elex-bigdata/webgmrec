package com.elex.webgamerec.ETL;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.elex.webgamerec.comm.Constants;
import com.elex.webgamerec.comm.HdfsUtils;
import com.elex.webgamerec.comm.PropertiesUtils;

public class InputMerger extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new InputMerger(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"input-merger");
		
		job.setJarByClass(InputMerger.class);		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat.class);
		Path in = new Path(PropertiesUtils.getRootDir()+Constants.INPUTDIR);
		FileInputFormat.addInputPath(job, in);
		
		job.setOutputFormatClass(TextOutputFormat.class);		
		String output = PropertiesUtils.getRootDir()+Constants.INPUTMERGEDIR;
		HdfsUtils.delFile(fs, output);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		return job.waitForCompletion(true)?0:1;
	}
		
	public static class MyMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

		
		@Override
		protected void map(Text key, IntWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(new Text(key.toString().substring(0, key.toString().lastIndexOf(","))), value);	
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, IntWritable, Text, Text> {
		private int sum,count;
		private String uid,gid,gmType,lang;
		private String[] vList;
		private Double dayAvg;
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			sum = 0;
			count = 0;
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
				count++;
			}
			
			if(gmType != null){
				if(gmType.equals("m")){
					context.write(null, new Text(uid+","+gid+","+sum+","+gmType+","+lang));
				}else if(gmType.equals("w")){
					dayAvg = new Double(sum)/new Double(count);
					context.write(null, new Text(uid+","+gid+","+dayAvg.intValue()+","+gmType+","+lang));
				}
			}						
		}
		
	}		
}
