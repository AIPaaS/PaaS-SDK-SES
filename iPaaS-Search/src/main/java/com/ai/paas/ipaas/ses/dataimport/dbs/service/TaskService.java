package com.ai.paas.ipaas.ses.dataimport.dbs.service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.search.service.ISearchClient;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.ExportFormatConfig;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.TableRuleConfig;
import com.ai.paas.ipaas.ses.dataimport.impt.model.Result;
import com.ai.paas.ipaas.ses.dataimport.util.SesUtil;

/**
 * 采用流水线形式
 * 两级queue
 *一个生产者线程、多个消费者线程、多个提交者线程
 */
public class TaskService {
	private static final Logger log = LoggerFactory.getLogger(TaskService.class);
	
	public Result start(final TableRuleConfig config,
			final ExportFormatConfig exportFormatConfig) throws Exception{
		// 表分片数
		int tablePairs = 0;
		for (String dbName : config.getLogicDbMap().keySet()) {
			TableRuleConfig.LogicDb logicDb = config.getLogicDbMap()
					.get(dbName);
			tablePairs += logicDb.tables.size();
		}
        log.debug(" tablePairs {}", tablePairs);
		//Runtime.getRuntime().availableProcessors() * 2
		ExecutorService executorService = new ThreadPoolExecutor(0,tablePairs, 10L, TimeUnit.SECONDS,
				new SynchronousQueue<Runnable>(true));

		final CountDownLatch latch = new CountDownLatch(tablePairs);
		
		final Result result = new Result();
		
		ISearchClient is = SesUtil.getSesClient(exportFormatConfig.getSesUserInfo());
		is.cleanData();
		
		
		for (String dbName : config.getLogicDbMap().keySet()) {
			final TableRuleConfig.LogicDb logicDb = config.getLogicDbMap()
					.get(dbName);
			for (final TableRuleConfig.TablePair tablePair : logicDb.tables) {
				executorService.execute(new TablePairProductor(logicDb,
						exportFormatConfig,tablePair,latch,result));
			}
		}
		log.info("wait for finished");
		try {
			latch.await();
		} catch (InterruptedException ignored) {
			log.error("InterruptedException", ignored);
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
			try {
				executorService.awaitTermination(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				log.error("InterruptedException",e);
			}
		}
		log.info("finished task");

		return result;

	}

}
