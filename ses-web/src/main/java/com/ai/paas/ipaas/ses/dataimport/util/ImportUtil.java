package com.ai.paas.ipaas.ses.dataimport.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ai.paas.ipaas.ses.dataimport.impt.model.Result;

/**
 * 辅助导入进度
 */
public class ImportUtil {
	private static Map<String, Result> running = new ConcurrentHashMap<String, Result>();

	public static void setRunning(String sesInfo, Result result) {
		running.put(sesInfo, result);
	}

	public static void removeRunning(String sesInfo) {
		running.remove(sesInfo);
	}

	public static Result getRunning(String sesInfo) {
		return running.get(sesInfo);
	}
}
