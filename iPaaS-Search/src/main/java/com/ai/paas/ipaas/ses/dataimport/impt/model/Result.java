package com.ai.paas.ipaas.ses.dataimport.impt.model;

import java.util.Iterator;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

import com.ai.paas.ipaas.ses.dataimport.util.DateUtil;

public class Result {
	//异常记录
	private TransferQueue<String> excLogs = new LinkedTransferQueue<String>() ;
	//总记录数
	private TransferQueue<Integer> total = new LinkedTransferQueue<Integer>() ;
	//成功记录数
	private TransferQueue<Integer> successTotal = new LinkedTransferQueue<Integer>() ;

	
	public void addExcLogs(String msg){
		excLogs.add(msg);
	}
	public void addTotal(int num){
		total.add(num);
	}
	public void addSucTotal(int successNum){
		successTotal.add(successNum);
	}
	public TransferQueue<String> getExcLogs() {
		return excLogs;
	}
	public TransferQueue<Integer> getTotal() {
		return total;
	}
	public TransferQueue<Integer> getSuccessTotal() {
		return successTotal;
	}
		
	public int getTotalNum(){
		int totalN = 0;
		Iterator<Integer> iterator = total.iterator();
		while(iterator.hasNext()){
			totalN += iterator.next();
		}
		return totalN;
	}
	public int getSucTotal(){
		int totalN = 0;
		Iterator<Integer> iterator = successTotal.iterator();
		while(iterator.hasNext()){
			totalN += iterator.next();
		}
		return totalN;
	}
	
	public String getExcLog(){
		StringBuffer str = new StringBuffer();
		Iterator<String> iterator = excLogs.iterator();
		while(iterator.hasNext()){
			if(str.length()>0)
				str.append(";");
			str.append(iterator.next());
		}
		return str.toString();
	}
	
	//poll         移除并返问队列头部的元素    如果队列为空，则返回null
	public String getLastExcLog(){
		StringBuffer str = new StringBuffer();
		String ss = null;
		while( (ss = excLogs.poll())!=null){
			str.append("<p>").append(ss).append("</p>");
		}
		return str.toString();
	}

}
