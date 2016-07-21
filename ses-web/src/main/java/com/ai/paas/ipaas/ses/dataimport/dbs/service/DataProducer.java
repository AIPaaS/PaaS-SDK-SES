package com.ai.paas.ipaas.ses.dataimport.dbs.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TransferQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.ConnectionFactory;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.ExportFormatConfig;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.TableRuleConfig;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.TableRuleConfig.LogicDb;
import com.ai.paas.ipaas.ses.dataimport.impt.model.Result;
import com.ai.paas.ipaas.ses.dataimport.util.ParamUtil;
import com.ai.paas.ipaas.vo.ses.SesIndexSqlInfo;
import com.mysql.jdbc.Statement;

public class DataProducer implements Runnable{
	private static final Logger log = LoggerFactory.getLogger(DataProducer.class);
	
	private TableRuleConfig.LogicDb logicDb;
	private ExportFormatConfig ec;
	private TableRuleConfig.TablePair tablePair;
	private int maxQueueSize = 8192;
	private int pageSize = 20;
	private TransferQueue<String> dataBase = null;


	private CountDownLatch producerOverLatch;
	private Result result;
	
	public DataProducer(final TableRuleConfig.LogicDb logicDb,
			final ExportFormatConfig ec,final TableRuleConfig.TablePair tablePair,
			int pageSize,TransferQueue<String> dataBase,int maxQueueSize,
			CountDownLatch producerOverLatch,
			Result result){
		this.logicDb = logicDb;
		this.ec = ec;
		this.tablePair = tablePair;
		this.pageSize = pageSize;
		this.maxQueueSize = maxQueueSize;
		this.producerOverLatch = producerOverLatch;
		this.dataBase = dataBase;
		this.result = result;
		
	} 

	private void readDbFromDBS(final LogicDb logicDb, final ExportFormatConfig ec,
			final TableRuleConfig.TablePair tablePair) throws Exception {

		Connection conn = null;
		Statement statement =null ;
		String sql = null;
		ResultSet rs = null;
		int totalNum = 0;
		try {
			if (conn == null) {
			    String url;
			    url = logicDb.masterUrl;
			    conn = ConnectionFactory.getConn(url);
			    statement = ConnectionFactory.createStatement(conn);
			    if (conn == null || statement == null) {
			        throw new Exception("Connection is null.");
			    }
			}

			log.info("Read table :{}", tablePair);
			sql = ec.getDataSql().getPrimarySql().getSql().replace(ec.getTables()[0], "{0}");
			sql = ParamUtil.fillStringByArgs(sql,new String[]{tablePair.realTableName});
			String totalSql = getTotalSql(sql);
			rs = statement.executeQuery(totalSql);
			rs.next();
			totalNum = rs.getInt(1);
		} catch (Exception e) {
			log.error("--------------db exception:"+e.getMessage(),e);
		} finally {
			try {
				if(rs!=null)
					rs.close();
			} catch (SQLException e) {
				log.error("--------------db close resultSet exception:"+e.getMessage(),e);
			}
			try {
				if(statement!=null)
					statement.close();
			} catch (SQLException e) {
				log.error("--------------db close preparedStatement exception:"+e.getMessage(),e);
			}
			try {
				if(conn!=null)
					conn.close();
			} catch (SQLException e) {
				log.error("--------------db close preparedStatement exception:"+e.getMessage(),e);
			}
		}
         
        
//		totalNum = 20000;
        result.addTotal(totalNum);
        //次数
        final int hNum = totalNum%pageSize==0?totalNum/pageSize:(totalNum/pageSize+1);
        log.info(" totalNum {},hNum {},pageSize {}", totalNum,hNum,pageSize);
        Connection mconn = null;
		mconn = ConnectionFactory.getConn(logicDb.masterUrl);
		if (mconn == null) 
            throw new Exception("m Connection is null.");
		for(int i=0;i<hNum;i++){
			 try {
             	excute(mconn,totalNum,hNum,pageSize,i,sql);
             } catch (Exception e) {
                 log.error("execute exception ", e);
             }
		}

		try {
			if(mconn!=null && !mconn.isClosed())
				mconn.close();
		} catch (SQLException e) {
			log.error("--------------db close preparedStatement exception:"+e.getMessage(),e);
		}
		producerOverLatch.countDown();
		log.info("--------------DataProducer------ producerOverLatch");

	}

	private String getTotalSql(String sql) {
		int start = sql.indexOf("select ")+"select ".length();
		int end = sql.indexOf(" from") ;
		String filed = sql.substring(start,end).trim();
		return sql.replace(filed, " count(*) as totalNum ");
	}


	@Override
	public void run() {
		if(ec.getType()==SesDataImportConstants.COMMON_DB_TYPE){
    		
    	}else{
    		try {
				readDbFromDBS(logicDb,ec,tablePair);
			} catch (Exception e) {
                log.error("execute exception ", e);
			}
    	}		
	}
	
	
	public void excute(
			Connection conn ,int totalNum, int threadNum, int rowNum, int i, String sql) throws Exception{
		long begin = System.currentTimeMillis();
		Statement statement = null;
		ResultSet rs = null;
		try {
			if((i+1)%100000==0){
				try {
					if(conn!=null){
						conn.close();
					}
				} catch (SQLException e) {
					log.error("--------------db close preparedStatement exception:"+e.getMessage(),e);
				}
				if(conn==null||conn.isClosed()){
					conn = null;
					conn = ConnectionFactory.getConn(logicDb.masterUrl);
				}
			}
			statement = (Statement) conn.createStatement();
			SesIndexSqlInfo dataSql = ec.getDataSql();
			@SuppressWarnings("unused")
			String priKey = dataSql.getPrimarySql().getPrimaryKey();
			rs = statement.executeQuery(getSql(rowNum, threadNum,totalNum,i,sql));
			StringBuffer data = new StringBuffer();
			try {
				while (rs.next()) {
					if(data.length()>0)
						data.delete(0,data.length());
					data.append("{");
					for (int j = 0; j < rs.getMetaData().getColumnCount(); j++) {
						data.append("\"").append(rs.getMetaData().getColumnName(j + 1)
								.toLowerCase()).append("\":\"").append(rs.getString(j + 1)).append("\",");
		            }
					data.deleteCharAt(data.length()-1);
					data.append("}");
					toQueue(data.toString());
				}
			} catch (Exception e) {
				log.error("--------------get ResultSet exception:"+e.getMessage(),e);
			}
			log.info("total-----first--------sql---- "+(System.currentTimeMillis()-begin));

			
		} catch (Exception e) {
			log.error("-------------- exception:"+e.getMessage(),e);
			throw e;
		} finally {
			try {
				if(rs!=null)
					rs.close();
			} catch (SQLException e) {
				log.error("--------------db close resultSet exception:"+e.getMessage(),e);
			}
			try {
				if(statement!=null)
					statement.close();
			} catch (SQLException e) {
				log.error("--------------db close preparedStatement exception:"+e.getMessage(),e);
			}
		}

	}

	private void toQueue(String data) throws Exception {
		if (null == data || data.length() == 0)
			return;
		if (!dataBase.tryTransfer(data)) {
			// 没有直接交换出去
			// 只有加到队列，防止爆了
			if (dataBase.size() < maxQueueSize) {
				dataBase.put(data);
			} else {
				log.info("----"+this.toString()+"----dataBase.transfer---");
				dataBase.transfer(data);
			}
		}

	}

	private String getSql(int rowNum, int threadNum,int totalNum,int i,String sql) {
		int start = i * rowNum;
		int rowNum2 = rowNum;
		if (i == threadNum - 1) {
			rowNum2 = totalNum - i * rowNum;
		}
		String sqlTem = sql;
		if (sql.contains(" limit ")) {
			sqlTem = sql.substring(0, sql.indexOf(" limit "));
		}
		sqlTem += " limit " + start + "," + rowNum2;
		return sqlTem;
	}
}
