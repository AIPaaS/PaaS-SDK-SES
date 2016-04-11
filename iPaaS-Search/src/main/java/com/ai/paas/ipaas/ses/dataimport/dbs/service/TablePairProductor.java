package com.ai.paas.ipaas.ses.dataimport.dbs.service;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.ses.dataimport.dbs.config.ExportFormatConfig;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.TableRuleConfig;
import com.ai.paas.ipaas.ses.dataimport.impt.model.Result;

public class TablePairProductor implements Runnable{
	private static final Logger log = LoggerFactory.getLogger(TablePairProductor.class);

	private int maxQueueSize = 8192;
	private int commitSize = 1024;
	private int consumerCnt = 15;
	private int consumerIndexCnt = 8;

	private ExecutorService executorService = null;
	private ExecutorService consumerExtService = null;

	private CountDownLatch producerOverLatch = new CountDownLatch(1);
	private CountDownLatch indexOverLatch = new CountDownLatch(consumerIndexCnt);
	private CountDownLatch overLatch = new CountDownLatch(consumerCnt);
	private CountDownLatch allLatch = null;

	private int pageSize = 20;
	
	private TableRuleConfig.LogicDb logicDb;
	private ExportFormatConfig ec;
	private TableRuleConfig.TablePair tablePair;
	private Result result;
	public TablePairProductor(final TableRuleConfig.LogicDb logicDb,
			final ExportFormatConfig ec,final TableRuleConfig.TablePair tablePair,
			CountDownLatch allLatch,
			Result result){
		this.logicDb = logicDb;
		this.ec = ec;
		this.tablePair = tablePair;
		this.allLatch = allLatch;
		this.result = result;
	}

	@Override
	public void run() {
		long start = System.currentTimeMillis();
		TransferQueue<String> dataBase = new LinkedTransferQueue<String>();
		TransferQueue<String> dataDoc = new LinkedTransferQueue<String>();
		executorService = Executors.newFixedThreadPool(consumerIndexCnt+1);
		// 启动一个生产者线程
		executorService.execute(new DataProducer(logicDb,
				ec,tablePair,
				pageSize,dataBase, maxQueueSize,
				producerOverLatch,result));
		// 启动多个消费者线程
		consumerExtService = Executors.newFixedThreadPool(consumerCnt);
		for (int i = 0; i < consumerCnt; i++) {
			consumerExtService.execute(new DataConsumer(ec,
					dataBase, dataDoc,
					overLatch, producerOverLatch,maxQueueSize));
		}
		// 启动多个提交者线程
		for (int i = 0; i < consumerIndexCnt; i++) {
			executorService.execute(new DataIndexConsumer(dataDoc,
					commitSize, overLatch, indexOverLatch,ec.getSesUserInfo(),result));
		}
		
		try {
			indexOverLatch.await();
		} catch (InterruptedException e) {
			log.error("doc producer error!tablePair=" + tablePair.logicTableName, e);
		}
		allLatch.countDown();
		log.info("data Info  producer over! tablePair=" + tablePair.logicTableName
				+ ", totalTime=" + (System.currentTimeMillis() - start));
		
	}

}
