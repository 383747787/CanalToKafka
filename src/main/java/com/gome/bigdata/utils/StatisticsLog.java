package com.gome.bigdata.utils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StatisticsLog {
	public static void recordReciver(String path,String type)  {
		InputStream in = null;
		OutputStream fos = null;
		Properties propRead = new Properties();
		Properties propWriter = new Properties();
		try{
			
			in = new FileInputStream(path);
			propRead.load(in);
			Enumeration enum1 = propRead.propertyNames();// 得到配置文件的名字
			while (enum1.hasMoreElements()) {
				String strKey = (String) enum1.nextElement();
				String strValue = propRead.getProperty(strKey);
				Integer strValueNew = null;
				if(strKey.equals(type)){
					strValueNew =Integer.parseInt(strValue)+1;
					propWriter.setProperty(strKey,strValueNew.toString());
				}else{
					propWriter.setProperty(strKey,strValue);
				}
			}
		}catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	    
		try{
			fos = new FileOutputStream(path);
	        propWriter.store(fos, "");
		}catch (Exception e) {
			e.printStackTrace();
		}finally{
			try {
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	} 
	
	public static void main(String[] args) {
		recordReciver("D:\\workspace\\flume_gome_wallet_sink\\src\\util\\gome_wallet_count_record.properties","SENDKAFKA");
	}
}
