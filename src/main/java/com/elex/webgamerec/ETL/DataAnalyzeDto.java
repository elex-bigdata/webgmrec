package com.elex.webgamerec.ETL;

import java.io.Serializable;

public class DataAnalyzeDto implements Serializable {

	
	private static final long serialVersionUID = -2141413054650389415L;

	private String gid;
	private String gt;
	private String language;
	private int sum;
	private int count;
	private int max;
	private int min;
	private Double avg;
	private Double percentile;
	private Double var;
	
	

	public DataAnalyzeDto(String gid, String gt, String language, int sum,
			int count, int max, int min, Double avg, Double percentile,Double var) {
		super();
		this.gid = gid;
		this.language = language;
		this.sum = sum;
		this.count = count;
		this.max = max;
		this.min = min;
		this.avg = avg;
		this.percentile = percentile;
		this.gt = gt;
		this.var = var;
	}
	
	public Double getVar() {
		return var;
	}

	public void setVar(Double var) {
		this.var = var;
	}
	
	public String getGt() {
		return gt;
	}


	public void setGt(String gt) {
		this.gt = gt;
	}
	
	public String getGid() {
		return gid;
	}
	public void setGid(String gid) {
		this.gid = gid;
	}
	public String getLanguage() {
		return language;
	}
	public void setLanguage(String language) {
		this.language = language;
	}
	public int getSum() {
		return sum;
	}
	public void setSum(int sum) {
		this.sum = sum;
	}
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public int getMax() {
		return max;
	}
	public void setMax(int max) {
		this.max = max;
	}
	public int getMin() {
		return min;
	}
	public void setMin(int min) {
		this.min = min;
	}
	public Double getAvg() {
		return avg;
	}
	public void setAvg(Double avg) {
		this.avg = avg;
	}
	public Double getPercentile() {
		return percentile;
	}
	public void setPercentile(Double percentile) {
		this.percentile = percentile;
	}
			
}
