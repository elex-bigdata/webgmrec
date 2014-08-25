package com.elex.webgamerec.comm;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomUtils {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	/**
	 * 随机指定范围内N个不重复的数
	 * 在初始化的无重复待�?数组中随机产生一个数放入结果中，
	 * 将待选数组被随机到的数，用待选数�?len-1)下标对应的数替换
	 * 然后从len-2里随机产生下�?��随机数，如此类推
	 * @param max  指定范围�?���?
	 * @param min  指定范围�?���?
	 * @param n  随机数个�?
	 * @return int[] 随机数结果集
	 */
	public static int[] randomArray(int min,int max,int n){
		int len = max-min+1;
		
		if(max < min || n > len){
			return null;
		}
		
		//初始化给定范围的待�?数组
		int[] source = new int[len];
        for (int i = min; i < min+len; i++){
        	source[i-min] = i;
        }
        
        int[] result = new int[n];
        Random rd = new Random();
        int index = 0;
        for (int i = 0; i < result.length; i++) {
        	//待�?数组0�?len-2)随机�?��下标
            index = Math.abs(rd.nextInt() % len--);
            //将随机到的数放入结果�?
            result[i] = source[index];
            //将待选数组中被随机到的数，用待�?数组(len-1)下标对应的数替换
            source[index] = source[len];
        }
        return result;
	}
	
	public static List<String> randomTopN(int n,List<String> top){
		List<String> result = new ArrayList<String>();
		int index[];
		if(n>top.size()){
			return top;
		}else{
			index = RandomUtils.randomArray(0, top.size() - 1, n);
			for (int i=0;i<index.length;i++) {
				result.add(top.get(index[i]));
			}
		}
		
		return result;
	}
}
