package com.ai.paas.ipaas.ses.dataimport.util;

public class SqlUtil {
	public static String getNumSize(int totalNum) {
		int threadNum = Runtime.getRuntime().availableProcessors() * 5;
		if (threadNum > 10)
			threadNum = 10;
		int pageSize = 5000;
		if (5000 >= totalNum) {
			pageSize = 600;
			threadNum = totalNum % pageSize == 0 ? totalNum / pageSize
					: (totalNum / pageSize + 1);
		} else if (30000 >= totalNum && totalNum > 5000) {
			pageSize = 3000;
			threadNum = totalNum % pageSize == 0 ? totalNum / pageSize
					: (totalNum / pageSize + 1);
		} else {
			if (totalNum % threadNum == 0) {
				pageSize = totalNum / threadNum;
			} else {
				pageSize = totalNum / threadNum;
				threadNum = threadNum + 1;
			}
		}
		return threadNum + "," + pageSize;
	}

	public static String getTotalSql(String sql) {
		int start = sql.indexOf("select ") + "select ".length();
		int end = sql.indexOf(" from");
		String filed = sql.substring(start, end).trim();
		return sql.replace(filed, " count(*) as totalNum ");
	}
}
