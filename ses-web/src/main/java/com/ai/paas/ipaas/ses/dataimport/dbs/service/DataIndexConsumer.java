package com.ai.paas.ipaas.ses.dataimport.dbs.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.search.ISearchClient;
import com.ai.paas.ipaas.ses.dataimport.impt.model.Result;
import com.ai.paas.ipaas.ses.dataimport.util.SesUtil;

public class DataIndexConsumer implements Runnable {
	private static final Logger log = LoggerFactory
			.getLogger(DataIndexConsumer.class);
	
	private CountDownLatch overLatch;
	private CountDownLatch indexOverLatch;
	private int commitSize = 1024;
	private TransferQueue<String> dataDoc;
	private boolean exist = false;

	private ISearchClient is;
	private Result result ;

	
	public DataIndexConsumer(TransferQueue<String> dataDoc,
			int commitSize, CountDownLatch overLatch, CountDownLatch indexOverLatch,
			String userInfo,Result result ){
		this.overLatch = overLatch;
		this.indexOverLatch = indexOverLatch;
		this.commitSize = commitSize;
		this.dataDoc = dataDoc;
		is = SesUtil.getSesClient(userInfo);
		this.result = result;
	}

	@Override
	public void run() {
		String dataDocIndex = null;
		List<String> dataTemp = new ArrayList<String>();
		int total = 0;
		// 什么时候退出，可以退出了
		while (!(dataDocIndex == null && exist)) {
			try {
				// 等待1秒，取不到返回null
				dataDocIndex = dataDoc.poll(1, TimeUnit.SECONDS);
				if (null != dataDocIndex) {
					dataTemp.add(dataDocIndex);
				}

				if (dataTemp.size() >= commitSize) {
					is.bulkJsonInsert(dataTemp);
					total += dataTemp.size();
					dataTemp.clear();
				}
			} catch (Exception e) {
				log.error("Doc es Index Consume Thread submit error!", e);
			} finally {
				try {
					if (!exist) {
						// 等待10毫秒，如果栓锁不为0
						exist = overLatch.await(10, TimeUnit.MICROSECONDS);
					}
				} catch (InterruptedException e) {
					log.error(
							"Doc es Index Consume Thread wait count down latch error!",
							e);
				}
			}
		}
		// 把最后的提交
		try {
			if (null != dataTemp && !dataTemp.isEmpty()) {
				is.bulkJsonInsert(dataTemp);
				total += dataTemp.size();
				dataTemp.clear();
			}
			result.addSucTotal(total);
		} catch (Exception e) {
			log.error("Doc es Index Consume Thread submit error!", e);
		}
		log.info("Total submitted  Doc:" + total);
		// 主线程可以退出了
		indexOverLatch.countDown();
		log.info("Doc es Index Consume Thread " + this.toString()
				+ " existed!");
	}
	
	

}
