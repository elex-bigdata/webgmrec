package com.elex.webgamerec;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.elex.webgamerec.ETL.DataAnalyzeUtils;
import com.elex.webgamerec.ETL.DataStandardize;
import com.elex.webgamerec.ETL.IDMapping;
import com.elex.webgamerec.ETL.InputCollector;
import com.elex.webgamerec.ETL.InputMerger;
import com.elex.webgamerec.ETL.PrepareInputForCF;
import com.elex.webgamerec.algorithm.ItemBaseCF;
import com.elex.webgamerec.algorithm.RecommendMixer;


public class Scheduler {

	private static final Logger log = LoggerFactory.getLogger(Scheduler.class);
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		AtomicInteger currentPhase = new AtomicInteger();
		String[] stageArgs = {otherArgs[0],otherArgs[1]};//运行阶段控制参数
		int success = 0;
		
		//stage=0 收集原始数据然后合并
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("RAW DATA COLLECT START!!!");
			success = rawDataCollect();
			if (success != 0) {
				log.error("RAW DATA COLLECT ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("RAW DATA COLLECT SUCCESS!!!");
		}
		
		//stage=1执行数据分析，数据标准化，id映射，格式转换等一系列过程
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("etl START!!!");
			success = etl();
			if (success != 0) {
				log.error("etl ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("etl SUCCESS!!!");
		}
		
		//stage=2 执行协同过滤并解析输出结果
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("CF START!!!");
			success = cf();
			if (success != 0) {
				log.error("CF ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("CF SUCCESS!!!");
		}
		
		
		//stage=3生成最终推荐结果
		if (shouldRunNextPhase(stageArgs, currentPhase)) {
			log.info("RecommendMixer START!!!");
			success = rec();
			if (success != 0) {
				log.error("RecommendMixer ERROR!!!,SYSTEM EXIT!!!");
				System.exit(success);
			}
			log.info("RecommendMixer SUCCESS!!!");
		}
														
	}
	
	private static int rec() throws Exception {

		return ToolRunner.run(new Configuration(), new RecommendMixer(), null);
	}

	private static int rawDataCollect() throws Exception {
		log.info("InputCollector START!!!");
		int a = ToolRunner.run(new Configuration(), new InputCollector(), null);
		log.info("InputCollector END!!!");
		
		log.info("InputMerger START!!!");
		int b = ToolRunner.run(new Configuration(), new InputMerger(), null);
		log.info("InputMerger END!!!");
		
		return Math.max(a, b);
	}
	
	private static int etl() throws Exception{
		
		log.info("DataAnalyzeUtils.analyze START!!!");
		int a = DataAnalyzeUtils.analyze();
		log.info("DataAnalyzeUtils.analyze END!!!");
		
		log.info("DataStandardize START!!!");
		int b = ToolRunner.run(new Configuration(), new DataStandardize(), null);
		log.info("DataStandardize END!!!");
		
		log.info("IDMapping.createIdMappingFile START!!!");
		int c = IDMapping.createIdMappingFile();
		log.info("IDMapping.createIdMappingFile END!!!");
		
		log.info("PrepareInputForCF.prepareInput START!!!");
		int d = PrepareInputForCF.prepareInput();
		log.info("PrepareInputForCF.prepareInput END!!!");
		
		log.info(a+","+b+","+c+","+d);
		
		return Math.max(a, Math.max(b, Math.max(c, d)));
	}
	
	private static int cf() throws Exception{
		
		log.info("ItemBaseCF START!!!");
		int a = ItemBaseCF.RunItemCf();
		log.info("ItemBaseCF END!!!");
		
		log.info("ItemBaseCF.recParse START!!!");
		int b = ItemBaseCF.recParse();
		log.info("ItemBaseCF.recParse END!!!");
		
		return Math.max(a, b);
	}
	
	
	protected static boolean shouldRunNextPhase(String[] args, AtomicInteger currentPhase) {
	    int phase = currentPhase.getAndIncrement();
	    String startPhase = args[0];
	    String endPhase = args[1];
	    boolean phaseSkipped = (startPhase != null && phase < Integer.parseInt(startPhase))
	        || (endPhase != null && phase > Integer.parseInt(endPhase));
	    if (phaseSkipped) {
	      log.info("Skipping phase {}", phase);
	    }
	    return !phaseSkipped;
	  }

}
