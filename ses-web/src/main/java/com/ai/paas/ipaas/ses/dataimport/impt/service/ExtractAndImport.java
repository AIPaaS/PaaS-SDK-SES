package com.ai.paas.ipaas.ses.dataimport.impt.service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.search.ISearchClient;
import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.ExportFormatConfig;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.TableRuleConfig;
import com.ai.paas.ipaas.ses.dataimport.impt.model.Result;
import com.ai.paas.ipaas.ses.dataimport.impt.task.ExtractAndImportTask;
import com.ai.paas.ipaas.ses.dataimport.impt.task.PairTask;
import com.ai.paas.ipaas.ses.dataimport.util.ConfUtil;
import com.ai.paas.ipaas.ses.dataimport.util.ImportUtil;
import com.ai.paas.ipaas.ses.dataimport.util.SesUtil;

/**
 * 单线程处理主表的每个分片，
 * 启动多线程补充字段、提交（每个二级线程流水做活，补充字段、批量提交）
 */
public class ExtractAndImport {
	private static final Logger log = LoggerFactory.getLogger(ExtractAndImport.class);
	
	public Result start(TableRuleConfig config,
			ExportFormatConfig exportFormatConfig) throws Exception{
		// 表分片数
		int tablePairs = 0;
		for (String dbName : config.getLogicDbMap().keySet()) {
			TableRuleConfig.LogicDb logicDb = config.getLogicDbMap()
					.get(dbName);
			tablePairs += logicDb.tables.size();
		}
        log.debug(" tablePairs {}", tablePairs);
		ExecutorService executorService = new ThreadPoolExecutor(0,tablePairs, 10L, TimeUnit.SECONDS,
				new SynchronousQueue<Runnable>(true));

		final CountDownLatch latch = new CountDownLatch(tablePairs);
		
		final Result result = new Result();
		
		String[] userInfos = exportFormatConfig.getSesUserInfo().split(",");
		ISearchClient is = null;
		try {
			is = SesUtil.getSESInstance(userInfos[0], userInfos[1]
					,userInfos[2],ConfUtil.getProperty("AUTH_ADDR_URL")+"/auth");
//			is.cleanData();
		} catch (Exception e) {
			log.error("SES service error...",e);
			throw e;
		}
		ImportUtil.setRunning(userInfos[0]+SesDataImportConstants.SPLIT_STR+
				userInfos[1], result);
		
		for (String dbName : config.getLogicDbMap().keySet()) {
			final TableRuleConfig.LogicDb logicDb = config.getLogicDbMap()
					.get(dbName);
			for (final TableRuleConfig.TablePair tablePair : logicDb.tables) {
				executorService.execute(new PairTask(logicDb,
						exportFormatConfig,tablePair,latch,result,is));
			}
		}
		log.debug("wait for finished");
		try {
			latch.await();
		} catch (InterruptedException ignored) {
			log.error("latch.await--InterruptedException",ignored);
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
			try {
				executorService.awaitTermination(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				log.error("InterruptedException",e);
			}
		}
		log.debug("finished task");
		ImportUtil.removeRunning(userInfos[0]+SesDataImportConstants.SPLIT_STR+
				userInfos[1]);
		return result;

	}
	
	
	
	/**
	 * 普通数据库
	 * @param config
	 * @param exportFormatConfig
	 * @return
	 * @throws Exception
	 */
	public Result start4Common(TableRuleConfig config,
			ExportFormatConfig exportFormatConfig) throws Exception{
        log.debug(" start4Common");
		final Result result = new Result();
		String[] userInfos = exportFormatConfig.getSesUserInfo().split(",");
		ISearchClient is = null;
		try {
			is = SesUtil.getSESInstance(userInfos[0], userInfos[1]
					,userInfos[2],ConfUtil.getProperty("AUTH_ADDR_URL")+"/auth");
//			is.cleanData();
		} catch (Exception e) {
			log.error("SES service error...",e);
			throw e;
		}
		ImportUtil.setRunning(userInfos[0]+SesDataImportConstants.SPLIT_STR+
				userInfos[1], result);
		
		final CountDownLatch latch = new CountDownLatch(1);
		ExecutorService executorService = Executors.newSingleThreadExecutor();
		// 启动一个线程
		executorService.execute(new ExtractAndImportTask(null,
				exportFormatConfig,null,latch,result,is));
		log.debug("wait for start4Common finished");
		try {
			latch.await();
		} catch (InterruptedException ignored) {
			log.error("latch.await--InterruptedException",ignored);
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
			try {
				executorService.awaitTermination(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				log.error("InterruptedException",e);
			}
		}
		log.debug("finished start4Common task");
		ImportUtil.removeRunning(userInfos[0]+SesDataImportConstants.SPLIT_STR+
				userInfos[1]);
		return result;

	}

}
