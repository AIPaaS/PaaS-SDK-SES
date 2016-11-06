package com.ai.paas.ipaas.ses.dataimport.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
	private static SimpleDateFormat dateFormat_hms = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ");
	
	public static String getTimes(){
		return dateFormat_hms.format(new Date());
	}

	public static void main(String[] args){
		System.out.print(getTimes());
	}
}
