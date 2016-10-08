package com.ai.paas.ipaas.ses.dataimport.util;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

/** 
 * 基于注解的定时器 
 * @author sunhz 
 */  
@Component  
public class LogListener {
	private static final Logger logger = LogManager.getLogger(LogListener.class.getName());

	/**  
     * 定时计算。每天凌晨 01:00 执行一次  
     */    
    //@Scheduled(cron = "0 0 1 * * *")   
    public void run() {
		logger.info("---【监听器监听到开始删除日志文件】---");
		
		// 获取日志文件夹的路径
		String logPath = "logs/";
		/**
		 * 定时删除任务
		 */
		// 获取当前日期往前推3天的时间
		String delDate = getDelDateByDay(3);

		// 首先进入目录去匹配是否有该文件夹
		if (new File(logPath).isDirectory()) {
			// 获取文件夹中的文件集合
			File[] logs = new File(logPath).listFiles();
			// 遍历集合
			for (int i = 0; i < logs.length; i++) {
				// 获取第i个文件
				File log = logs[i];
				logger.info("==========文件名： "+ log.getName() +"\r\n");
				// 获取第i个文件的名称，若属于被删除日期的日志则删除
				if (log.getName().contains(delDate)) {
					logger.info("----监听器中，开始删除3天前的日志文件:" + log);
					// 执行删除方法
					log.delete();
				}
			}
		}
		logger.info("---【监听器监听到删除日志文件结束】---");
	}
    
    
    /**
	 * 从当前日期算起，获取N天前的日期（当前日不算在内），日期格式为yyyy-MM-dd
	 *
	 * @param daily 天数
	 * @return
	 */
	public static String getDelDateByDay(Integer daily) {
	    Date date = new Date();
	    int year = Integer.parseInt(new SimpleDateFormat("yyyy").format(date));
	    int month = Integer.parseInt(new SimpleDateFormat("MM").format(date));
	    int day = Integer.parseInt(new SimpleDateFormat("dd").format(date)) - daily;
	    while (day < 1) {
	        month -= 1;
	        if (month == 0) {
	            year -= 1;
	            month = 12;
	        }
	        if (month == 4 || month == 6 || month == 9 || month == 11) {
	            day = 30 + day;
	        } else if (month == 1 || month == 3 || month == 5 || month == 7
	                || month == 8 || month == 10 || month == 12) {
	            day = 31 + day;
	        } else if (month == 2) {
	            if (year % 400 == 0 || (year % 4 == 0 && year % 100 != 0)) {
	                day = 29 + day;
	            }
	            else {
	                day = 28 + day;
	            }
	        }
	 
	    }
	    String y = year + "";
	    String m = "";
	    String d = "";
	    if (month < 10) {
	        m = "0" + month;
	    } else {
	        m = month + "";
	    }
	    if (day < 10) {
	        d = "0" + day;
	    } else {
	        d = day + "";
	    }
	    return y + "-" + m + "-" + d;
	}
}