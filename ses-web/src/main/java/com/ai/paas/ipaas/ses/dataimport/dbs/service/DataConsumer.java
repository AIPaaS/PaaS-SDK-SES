package com.ai.paas.ipaas.ses.dataimport.dbs.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TransferQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.ExportFormatConfig;
import com.ai.paas.ipaas.ses.dataimport.model.DataBaseAttr;
import com.ai.paas.ipaas.ses.dataimport.model.FiledSql;
import com.ai.paas.ipaas.ses.dataimport.util.GsonUtil;
import com.ai.paas.ipaas.ses.dataimport.util.JdbcUtil;
import com.ai.paas.ipaas.txs.dtm.TransactionContext;

public class DataConsumer implements Runnable{
	
	private static final Logger log = LoggerFactory.getLogger(DataConsumer.class);

	
	private ExportFormatConfig ec;
	
	private TransferQueue<String> dataBase;
	private TransferQueue<String> dataDoc;
	private CountDownLatch overLatch;
	private CountDownLatch producerOverLatch;
	private int maxQueueSize = 8192;
	private boolean exist = false;
	
	public DataConsumer(
			ExportFormatConfig ec,
			TransferQueue<String> dataBase,TransferQueue<String> dataDoc,
			CountDownLatch overLatch,CountDownLatch producerOverLatch,
			int maxQueueSize ){
		this.ec = ec;
		this.overLatch = overLatch;
		this.producerOverLatch = producerOverLatch;
		this.dataBase = dataBase;
		this.dataDoc = dataDoc;
		this.maxQueueSize = maxQueueSize;
		
	} 

	@Override
	public void run() {
		int total = 0;
		// 从队列获取信息
		String baseInfo = null;
		// 即使在退出条件满足时也要取一次
		String doc = null;
		while (!(baseInfo == null && exist)) {
			try {

				baseInfo = dataBase.poll(1, TimeUnit.SECONDS);
				// 转换，补齐信息
				if (null != baseInfo) {
					// 转换
					doc = writeFileds(baseInfo);
					total++;
					// 放到队列
					if (!dataDoc.tryTransfer(doc)) {
						// 没有直接交换出去
						// 只有加到队列，防止爆了
						if (dataDoc.size() < maxQueueSize) {
							dataDoc.put(doc);
						} else {
							log.info("----"+this.toString()+"----dataDoc.transfer---");
							dataDoc.transfer(doc);
						}
					}
				}

			} catch (Exception e) {
				log.error("baseInfo Consume Thread wait count down latch error!",
						e);
			} finally {
				try {
					// 等待10毫秒，如果栓锁不为0
					if (!exist) {
						exist = producerOverLatch.await(10,
								TimeUnit.MICROSECONDS);
					}
				} catch (InterruptedException e) {
					log.error(
							"GdsInfo Consume Thread wait count down latch error!",
							e);
				}
			}

		}
		log.info("baseInfo Consume Thread " + this.toString()
				+ " convert doc num=" + total);
		// 每个消费者减一
		overLatch.countDown();
		// 可以退出了，退出
		log.info("baseInfo Consume Thread " + this.toString() + " existed!");
		
	}
	
	private String writeFileds(String baseInfo) {
		HashMap<String,Object> datas = GsonUtil.gsonToObject(baseInfo, HashMap.class);
		
		for(Map.Entry<String,Object> entry : datas.entrySet()){
			String cv = entry.getValue().toString();
    		final String af = ec.getDataSql().getPrimarySql().getAlias()+"."+entry.getKey();
    		if(ec.getAfIndexFiledSqls().containsKey(af)){
    			List<FiledSql> fss = ec.getAfIndexFiledSqls().get(af);
    			for(FiledSql fs:fss){
    				getFiledValueUseIndex(datas,fs,entry.getKey(),cv,af);
    			}
    		}
    		if(ec.getAfFiledSqls().containsKey(af)){
    			List<FiledSql> fss = ec.getAfFiledSqls().get(af);
    			for(FiledSql fs:fss){
    				try {
						getFiledValue(datas,fs,entry.getKey(),cv,af);
					} catch (Exception e) {
						log.error("--------------getFiledValue exception:"+e.getMessage(),e);
					}
    			}
    		}
		}
		return GsonUtil.objToGson(datas);
	}

	private void getFiledValue(Map datas, FiledSql fs, String cn, String cv,
			String af) throws Exception {
		DataBaseAttr attr = ec.getDb().get(fs.getDrAlias());
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		List values = null;
		if (fs.getRelation() == SesDataImportConstants.ONE_TO_MANY_RELATION) {
			values = new ArrayList();
		}

		try {
			if (SesDataImportConstants.DBS_DB_TYPE==attr.getType()&&attr.isHaveTXS())
				TransactionContext.initVisualContext();
			conn = JdbcUtil.getConnection(attr);
			String temp = fs.getSql()
					.toLowerCase().replace(af, cv);
			preparedStatement = conn.prepareStatement(temp);
			
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
					String rcn = resultSet.getMetaData().getColumnName(i + 1)
							.toLowerCase();
					String rcv = resultSet.getString(i + 1);
					if (values != null) {
						values.add(rcv);
						datas.put(rcn, values);
					} else {
						datas.put(rcn, rcv);
					}
					final String saf = fs.getAlias() + "." + rcn;
					if (ec.getPafAfs().containsKey(saf)) {
						List<String> afs = ec.getPafAfs().get(saf);
						for (String tempAf : afs) {
							getFiledValue(datas,
									ec.getFiledSql().get(tempAf.split("\\.")[0]),
									rcn, rcv, saf);
						}
					}
				}
			}
			if (SesDataImportConstants.DBS_DB_TYPE==attr.getType()&&attr.isHaveTXS())
				TransactionContext.clear();
		} catch (Exception e) {
			log.error("--------------db exception:" + e.getMessage(), e);
			throw e;
		} finally {
			try {
				if (resultSet != null)
					resultSet.close();
			} catch (SQLException e) {
				log.error(
						"--------------db close resultSet exception:"
								+ e.getMessage(), e);
			}
			try {
				if (preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				log.error("--------------db close preparedStatement exception:"
						+ e.getMessage(), e);
			}
			JdbcUtil.closeConnection(conn);
		}

	}

	private void getFiledValueUseIndex(Map datas, FiledSql fs, String cn,
			String cv, String af) {
		DataBaseAttr attr = ec.getDb().get(fs.getDrAlias());
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try {
			if (attr.getType() == SesDataImportConstants.DBS_DB_TYPE)
				TransactionContext.initVisualContext();
			if (conn == null) {
				conn = JdbcUtil.getConnection(attr);
			}
			preparedStatement = conn.prepareStatement(fs.getIndexSql()
					.toLowerCase().replace(af, cv));
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
					String rcn = resultSet.getMetaData().getColumnName(i + 1)
							.toLowerCase();
					String rcv = resultSet.getString(i + 1);
					String sql = fs.getSql().toLowerCase();
					final String saf = fs.getIndexAlias() + "." + rcn;
					if (sql.contains(saf)) {
						getFiledValue(datas, fs, rcn, rcv, saf);
					}

				}
			}
			if (attr.getType() == SesDataImportConstants.DBS_DB_TYPE)
				TransactionContext.clear();
		} catch (Exception e) {
			try {
				JdbcUtil.removeConnection(attr.getId());
			} catch (SQLException e1) {
				log.error("--------------db exception:" + e1.getMessage(), e1);
			}
			log.error("--------------db exception:" + e.getMessage(), e);
		} finally {
			try {
				if (resultSet != null)
					resultSet.close();
			} catch (SQLException e) {
				log.error(
						"--------------db close resultSet exception:"
								+ e.getMessage(), e);
			}
			try {
				if (preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				log.error("--------------db close preparedStatement exception:"
						+ e.getMessage(), e);
			}
			try {
				JdbcUtil.closeConnection(conn);
			} catch (SQLException e1) {
				log.error("--------------db exception:" + e1.getMessage(), e1);
			}

		}

	}

}
