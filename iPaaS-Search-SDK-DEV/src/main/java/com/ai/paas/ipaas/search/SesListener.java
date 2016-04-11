package com.ai.paas.ipaas.search;




public class SesListener {

	public static void main(String[] args) {
		for(int i = 0 ;i<1 ;i++){
			TestInsert ti = new TestInsert(i);
			ti.start();
		}
	}
	
}
