package com.elex.webgamerec.algorithm;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.elex.webgamerec.ETL.DataAnalyzeDto;
import com.elex.webgamerec.ETL.DataAnalyzeUtils;
import com.elex.webgamerec.comm.Constants;
import com.elex.webgamerec.comm.HdfsUtils;
import com.elex.webgamerec.comm.PropertiesUtils;

public class RecommendMixer extends Configured implements Tool {


	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new RecommendMixer(), args);
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf,"recommend-mixer");
		job.setJarByClass(RecommendMixer.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		Path rating = new Path(PropertiesUtils.getRootDir()+Constants.STANDARDIZE);
		FileInputFormat.addInputPath(job, rating);	
		Path tagcfout = new Path(PropertiesUtils.getRootDir()+Constants.CFRECPARSE);
		FileInputFormat.addInputPath(job, tagcfout);		
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path output = new Path(PropertiesUtils.getRootDir()+Constants.CFREC);
		HdfsUtils.delFile(fs, output.toString());
		FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		String[] list;
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String pathName = ((FileSplit)context.getInputSplit()).getPath().toString();
			if(pathName.contains(Constants.STANDARDIZE)){
				list = value.toString().split(",");
				if(list != null){
					if(list.length == 5){
						if(list[3].equals("w")){
							context.write(new Text(list[0]), new Text("01_"+value.toString().substring(value.toString().indexOf(",")+1, value.toString().length())));
						}
					}					
				}
				
			}else if(pathName.contains(Constants.CFRECPARSE)){
				list = value.toString().split("\\s");
				if(list != null){
					if(list.length == 2){
						context.write(new Text(list[0]), new Text("02_"+list[1]));
					}
				}				
			}
		}				
				
	}		
	
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		Map<String,List<DataAnalyzeDto>> rank;
		Map<String,Double> recMap = new TreeMap<String,Double>();
		List<Map.Entry<String,Double>> result;
		String[] vList,kv;
		String lang;
		double rate;
		DecimalFormat df = new DecimalFormat("#.####");
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			
			rank = DataAnalyzeUtils.getRank();
		}

		
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
			int size = Integer.parseInt(PropertiesUtils.getCfNumOfRec());
			recMap.clear();
			for(Text line:values){
				if(line.toString().startsWith("01_")){
					vList = line.toString().substring(3, line.toString().length()).split(",");
					rate = recMap.get(vList[0])!=null?Math.max(rate, recMap.get(vList[0])):new Double(vList[1]);
					lang = vList[3];
					recMap.put(vList[0], rate);
				}else if(line.toString().startsWith("02_")){
					vList = line.toString().substring(3, line.toString().length()).split(",");
					for(int i=0;i<vList.length;i++){
						kv = vList[i].split(":");
						rate = recMap.get(kv[0])!=null?Math.max(rate, recMap.get(kv[0])):new Double(kv[1]);
						recMap.put(kv[0], rate);
					}										
				}				
			}
			
			if(lang != null){
				if(rank.get(lang) != null){
					List<DataAnalyzeDto> topN = rank.get(lang);
					Collections.sort(topN);
					for(int i=0;i<topN.size() && i<= PropertiesUtils.getTopN();i++){
						rate = recMap.get(topN.get(i).getGid())!=null?Math.max(Constants.RATE, recMap.get(topN.get(i).getGid())):Constants.RATE;
						recMap.put(topN.get(i).getGid(), rate);
					}					
				}
			}
			
			result = new ArrayList<Map.Entry<String,Double>>(recMap.entrySet());
			
			Collections.sort(result,new Comparator<Map.Entry<String,Double>>() {
	            //降序排序
				@Override
	            public int compare(Entry<String, Double> o1,
	                    Entry<String, Double> o2) {
	                return o2.getValue().compareTo(o1.getValue());
	            }	            
	        });
						
			if (result.size() > 0) {
										
				StringBuffer sb = new StringBuffer(200);
				sb.append(key.toString() + "\t");
				sb.append("[");
				for (int i = 0; i < result.size() && i < size; i++) {
					sb.append("{");
					sb.append("\"" + result.get(i).getKey() + "\":" + df.format(result.get(i).getValue()));
					sb.append("}");
					sb.append(",");
				}								

				context.write(null,new Text(sb.substring(0, sb.toString().length() - 1)+ "]"));
			}
									
		}				
		
	}
}
