package com.ai.paas.ipaas.ses.dataimport.impt.task;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.search.service.ISearchClient;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.ExportFormatConfig;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.TableRuleConfig;
import com.ai.paas.ipaas.ses.dataimport.impt.model.Result;

/**
 * 每个表分片任务：
 * 去读主表
 * 拼装fileds
 * toEs
 */
public class PairTask implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(PairTask.class);

	
	private TableRuleConfig.LogicDb logicDb;
	private ExportFormatConfig ec;
	private TableRuleConfig.TablePair tablePair;
	private CountDownLatch allLatch = null;
	
	private ExecutorService executorService = null;
	private Result result = null;
	private CountDownLatch exImLatch = new CountDownLatch(1);
	private ISearchClient is;

	
	private PairTask(){
		
	}
	
	public PairTask(final TableRuleConfig.LogicDb logicDb,
			final ExportFormatConfig ec,final TableRuleConfig.TablePair tablePair,
			CountDownLatch allLatch,Result result,
			ISearchClient is){
		this.logicDb = logicDb;
		this.ec = ec;
		this.tablePair = tablePair;
		this.allLatch = allLatch;
		this.result = result;
		this.is = is;
	}

	@Override
	public void run() {
		long start = System.currentTimeMillis();
		executorService = Executors.newSingleThreadExecutor();
		// 启动一个线程
		executorService.execute(new ExtractAndImportTask(logicDb,
				ec,tablePair,
				exImLatch,result,is));
		try {
			exImLatch.await();
		} catch (InterruptedException e) {
			log.error("exIm Task error!tablePair=" + tablePair.logicTableName, e);
		}
		allLatch.countDown();
		log.info("exIm Task over! tablePair=" + tablePair.logicTableName
				+ ", totalTime=" + (System.currentTimeMillis() - start));

	}

}
