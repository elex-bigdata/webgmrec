package com.elex.webgamerec.ETL;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

public class DataStandardize extends Configured implements Tool {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new DataStandardize(), args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"intput-standardization");
		
		job.setJarByClass(DataStandardize.class);		
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
		
		Path in = new Path(PropertiesUtils.getHiveWareHouse()+Constants.HIVEINPUTTABLE);
		FileInputFormat.addInputPath(job, in);
		
		job.setOutputFormatClass(TextOutputFormat.class);		
		String output = PropertiesUtils.getRootDir()+Constants.STANDARDIZE;
		HdfsUtils.delFile(fs, output);
		FileOutputFormat.setOutputPath(job, new Path(output));
		if(!DataAnalyzeUtils.dataAnalyze()){
			return 1;
		}
		return job.waitForCompletion(true)?0:1;
	}
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private String[] vList;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			vList = value.toString().split(",");
			if(vList != null){
				if(vList.length==5){
					context.write(new Text(vList[0]+","+vList[1]), new Text(vList[2]+","+vList[3]+","+vList[4]));
				}
			}
			
		}
		
	}
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		
		private Map<String,DataAnalyzeDto> anaMap;
		String gid,gt,lang,mixId;				
		String[] vList;
		Double rate,avg,sum;
		int count;
		DecimalFormat df = new DecimalFormat("#.###");
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			anaMap = DataAnalyzeUtils.getDataAnalyzeResult();
		}
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			count = 0;
			sum = 0D;
			gid=gt=lang=mixId=null;
			
			gid = key.toString()!=null?key.toString().split(",")[1]:null;
			
			for(Text v : values){
				vList = v.toString().split(",");
				if(vList != null){
					if(vList.length==3){						
						
						sum = sum+new Double(vList[0]);
						count++;
						gt = vList[1];
						lang = vList[2];
					}					
				}			
			}
			
			
			if(gid !=null && gt !=null && lang !=null){
				mixId = gid+gt+lang;
			}
			
			if(sum != 0D && count != 0 && mixId !=null){
				avg = sum/count;
				rate = anaMap.get(mixId).getPercentile()!=null?avg/anaMap.get(mixId).getPercentile():0D;
				rate = rate>1D?1D:rate;
				context.write(null, new Text(key.toString()+","+df.format(rate)));
			}
												
		}
		
	}
			
}
