package com.elex.webgamerec.algorithm;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.elex.webgamerec.ETL.FilterUtils;
import com.elex.webgamerec.ETL.IDMapping;



public class SimilarityParse extends Configured implements Tool {

	
	public static class MyMapper extends
			Mapper<IntWritable, VectorWritable, Text, Text> {

		private String fromKey;
		private String toKey;
		private String simValue;
		private DecimalFormat df = new DecimalFormat("#.###");
		private Iterator<Vector.Element> nonZeroElements = null;
		private Vector.Element nonZeroElement = null;
		private String[] gidMap;
		private Set<String> webGame;
		private Map<String, String> id_index_map = new HashMap<String, String>();
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			gidMap = IDMapping.getGids();
			webGame = FilterUtils.getWebGM();
			
			Configuration configuration = context.getConfiguration();			

			FileSystem fs = FileSystem.get(configuration);
			FileStatus[] files = fs.listStatus(new Path(configuration.get("id_index_file")));
			SequenceFile.Reader reader = null;
			for (FileStatus file : files) {
				if (!file.isDirectory()) {
					Path hdfs_src = file.getPath();
					if (file.getPath().getName().contains("part-r")) {
						reader = new SequenceFile.Reader(configuration, Reader.file(hdfs_src));
						Writable key = (Writable) ReflectionUtils.newInstance(
								reader.getKeyClass(), configuration);
						Writable value = (Writable) ReflectionUtils
								.newInstance(reader.getValueClass(),
										configuration);
						while (reader.next(key, value)) {
							id_index_map.put(key.toString(), value.toString());
						}
					}
				}
			}
		}

		

		public void map(IntWritable key, VectorWritable value, Context context)
				throws IOException, InterruptedException {
			
			nonZeroElements = value.get().nonZeroes().iterator();
			while (nonZeroElements.hasNext()) {
				nonZeroElement = nonZeroElements.next();
				fromKey = gidMap[new Integer(id_index_map.get(key.toString()))];
				toKey = gidMap[new Integer(id_index_map.get(Integer.toString(nonZeroElement.index())))];
				simValue = df.format(nonZeroElement.get());
				if(webGame.contains(fromKey) && webGame.contains(toKey) ){
					context.write(new Text(fromKey), new Text(toKey+","+simValue));
					context.write(new Text(toKey), new Text(fromKey+","+simValue));//因为输入目录只包含了相似度矩阵的上半部分，还需要输出下半部分
				}												
			}

		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		private int topN;
		private double range;		
		private List<ItemPrefDTO> itemPairList = new ArrayList<ItemPrefDTO>();
		private String[] itempref;
		private int i;
		private int loop;
		private String dtoStr;
		

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			itemPairList.clear();
			StringBuilder sb = new StringBuilder(200);
			i = 0;
			for (Text val : values) {
				itempref = val.toString().split(",");
				if (Double.parseDouble(itempref[1]) > range) {
					ItemPrefDTO dto = new ItemPrefDTO();
					dto.setDst_itemId(itempref[0].trim());
					dto.setPref(itempref[1].trim());
					itemPairList.add(dto);
				}
				
			}

			Collections.sort(itemPairList, new ItemPrefComparator());

			if (topN > itemPairList.size()) {
				loop = itemPairList.size();
			} else {
				loop = topN;
			}

			sb.append(key.toString()).append("\t");
			while (i < loop) {
				dtoStr =itemPairList.get(i).getDst_itemId()  + ":" + itemPairList.get(i).getPref();
				sb.append(dtoStr);
				if (i != loop-1) {
					sb.append(",");					
				}
				i++;
			}
			if(loop>0){
				context.write(null, new Text(sb.toString()));
			}
			

		}
				

		protected void setup(Context context) throws IOException,
				InterruptedException {			
			
			Configuration configuration = context.getConfiguration();
			topN = Integer.parseInt(configuration.get("topN"));
			range = Double.parseDouble(configuration.get("range")) / 100;					
		}
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new SimilarityParse(),args);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("range", args[3]);
		conf.set("topN", args[2]);
		conf.set("id_index_file", args[0]
				+ "/preparePreferenceMatrix/itemIDIndex");
		Job job = Job.getInstance(conf, "SimilarityParse");
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setJarByClass(SimilarityParse.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(MyReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]+ "/pairwiseSimilarity"));
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
}
