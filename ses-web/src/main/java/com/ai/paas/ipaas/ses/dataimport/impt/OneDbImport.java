package com.ai.paas.ipaas.ses.dataimport.impt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.search.service.ISearchClient;
import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.impt.model.Result;
import com.ai.paas.ipaas.ses.dataimport.util.ConfUtil;
import com.ai.paas.ipaas.ses.dataimport.util.GsonUtil;
import com.ai.paas.ipaas.ses.dataimport.util.ImportUtil;
import com.ai.paas.ipaas.ses.dataimport.util.JdbcUtil;
import com.ai.paas.ipaas.ses.dataimport.util.SesUtil;
import com.ai.paas.ipaas.ses.dataimport.util.SqlUtil;
import com.ai.paas.ipaas.vo.ses.SesDataSourceInfo;
import com.ai.paas.ipaas.vo.ses.SesIndexSqlInfo;

public class OneDbImport {
	private static final Logger log = LoggerFactory.getLogger(OneDbImport.class);
	private String sesUserInfo;
	private SesDataSourceInfo db;
	private SesIndexSqlInfo dataSql;
	private Result result;
	private ISearchClient is;
	private SimpleDateFormat dateFormat_hms = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	
	public OneDbImport(String sesUserInfo,SesDataSourceInfo db,SesIndexSqlInfo dataSql){
		this.dataSql = dataSql;
		this.db = db;
		this.sesUserInfo = sesUserInfo;
		result = new Result();
	}
	
	
	public Result start() throws Exception{
		String[] userInfos = sesUserInfo.split(",");
		try {
			is = SesUtil.getSESInstance(userInfos[0], userInfos[1]
					,userInfos[2],ConfUtil.getProperty("AUTH_ADDR_URL"));
//			is.cleanData();
		} catch (Exception e) {
			log.error("SES service error...",e);
			throw e;
		}
		ImportUtil.setRunning(userInfos[0]+SesDataImportConstants.SPLIT_STR+
				userInfos[1], result);
		
		Connection conn = null;
		PreparedStatement statement =null ;
		String sql = null;
		ResultSet rs = null;
		int totalNum = 0;
		try {
			conn = JdbcUtil.getConnection(db);
			sql = dataSql.getPrimarySql().getSql().toLowerCase();
			String totalSql = SqlUtil.getTotalSql(sql);
			statement =conn.prepareStatement(totalSql);
			log.debug("-------totalSql----{}------",totalSql);

		    if (conn == null || statement == null) {
		        throw new Exception("Connection is null.");
		    }
			log.debug("--OneDbImport--Read  begin ");
			rs = statement.executeQuery();
			rs.next();
	        totalNum = rs.getInt(1);
		} catch (Exception e) {
			result.addExcLogs("get db totalnums--"+e.getMessage());
			log.error("------OneDbImport--------db exception:"+e.getMessage(),e);
			try {
				Thread.sleep(1001);
			} catch (InterruptedException i) {
				log.error("--------------db exception--",i);
			}
		} finally {
			try {
				if(rs!=null)
					rs.close();
			} catch (SQLException e) {
				log.error("------OneDbImport--------db close resultSet exception:"+e.getMessage(),e);
			}
			try {
				if(statement!=null)
					statement.close();
			} catch (SQLException e) {
				log.error("--------OneDbImport------db close preparedStatement exception:"+e.getMessage(),e);
			}
			try {
				if(conn!=null)
					conn.close();
			} catch (SQLException e) {
				log.error("-------OneDbImport-------db close preparedStatement exception:"+e.getMessage(),e);
			}
		}
        result.addTotal(totalNum);
        if(totalNum>0){
	        String numSize = SqlUtil.getNumSize(totalNum);
	        int threadNum = Integer.valueOf(numSize.split(",")[0]).intValue();
	        int pageSize = Integer.valueOf(numSize.split(",")[1]).intValue();
	        
	        log.debug(" totalNum {},threadNum {},pageSize {}", totalNum,threadNum,pageSize);
	        
	        ExecutorService executorService = new ThreadPoolExecutor(0,threadNum, 10L, TimeUnit.SECONDS,
					new SynchronousQueue<Runnable>());
			final CountDownLatch taskLatch = new CountDownLatch(threadNum);
	
			for(int i=0;i<threadNum;i++){
				executorService.execute(new ExtractImTask(totalNum,threadNum,pageSize,i,dataSql.getPrimarySql().getSql(),taskLatch));
			}
			log.debug("--OneDbImport--wait for ExtractImTask thread finished");
			try {
				taskLatch.await();
			} catch (InterruptedException ignored) {
				log.error("taskLatch.await",ignored);
			}
			executorService.shutdown();
			while (!executorService.isTerminated()) {
				try {
					executorService.awaitTermination(10, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					log.error("ExtractImTask InterruptedException",e);
				}
			}
			log.debug("--OneDbImport--ExtractImTask thread finished task");
			ImportUtil.removeRunning(userInfos[0]+SesDataImportConstants.SPLIT_STR+
					userInfos[1]);
		}
		return result;
		
	}
	
	private class ExtractImTask implements Runnable{
		private int threadNum;
		private int pageSize;
		private int num;
		private int totalNum;
		private String sql;
		private CountDownLatch taskLatch;
		@SuppressWarnings("unused")
		private ExtractImTask(){}
		public ExtractImTask(int totalNum,int threadNum,int pageSize,int num,
				String sql,CountDownLatch taskLatch){
			this.threadNum = threadNum;
			this.pageSize = pageSize;
			this.num = num;
			this.sql = sql;
			this.totalNum = totalNum;
			this.taskLatch = taskLatch;
		}

		@Override
		public void run() {
			try {
				excute(totalNum,threadNum,pageSize,num,sql);
			} catch (Exception e) {
				log.error("--------------ExtractImTask excute exception:"+e.getMessage(),e);
			}
			taskLatch.countDown();
		}
		
		
		public void excute(
				int totalNum, int threadNum, int rowNum, int i, String sql) throws Exception{
			Connection conn = null;
			PreparedStatement statement = null;
			ResultSet rs = null;
			try {
				conn = JdbcUtil.getConnection(db);
				if (conn == null) {
	                throw new Exception("Connection is null.");
				}
				
				Map<String, String> datas = new HashMap<String, String>();
	            List<String> values = new ArrayList<String>();
	            String preSql = getSql(rowNum, threadNum,totalNum,i,sql);
				log.debug("-------preSql----{}------",preSql);
				statement = conn.prepareStatement(preSql);
				rs = statement.executeQuery();
				try {
					while (rs.next()) {
						datas.clear();
						for (int j = 0; j < rs.getMetaData().getColumnCount(); j++) {
							String cv = "";
							Object obj = rs.getObject(j + 1);
							if(obj instanceof  java.util.Date || obj instanceof  java.sql.Date || obj instanceof java.sql.Timestamp){
								cv = dateFormat_hms.format(obj);
							}else{
								cv = rs.getString(j + 1);
							}
							
							datas.put(rs.getMetaData().getColumnLabel(j+1).toLowerCase(), cv);
			            }
						values.add(GsonUtil.objToGson(datas));
						if(values.size()>=1024){
							is.bulkInsert(values);
							result.addSucTotal(values.size());
							values.clear();
						}
					}
					if(!values.isEmpty()){
						is.bulkInsert(values);
						result.addSucTotal(values.size());
						values.clear();
					}
				} catch (Exception e) {
					log.error("--------------get ResultSet exception:"+e.getMessage(),e);
					try {
						result.addExcLogs(e.getMessage());
					} catch (Exception ee) {
						log.error("--------------addExcLogs exception:"+ee.getMessage(),ee);
					}
				}
				
			} catch (Exception e) {
				log.error("-------------- exception:"+e.getMessage(),e);
				result.addExcLogs(e.getMessage());
				try {
					Thread.sleep(1001);
				} catch (InterruptedException ie) {
					log.error("--------------db exception--",ie);
				}
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
				try {
					if(conn!=null)
						conn.close();
				} catch (SQLException e) {
					log.error("--------------db close preparedStatement exception:"+e.getMessage(),e);
				}
			}

		}
		
		private String getSql(int rowNum, int threadNum,int totalNum,int i,String sql) {
			int start = i * rowNum;
			int rowNum2 = rowNum;
			if (i == threadNum - 1) {
				rowNum2 = totalNum - i * rowNum;
			}
			String sqlTem = sql.toLowerCase();
			if (sql.contains(" limit ")) {
				sqlTem = sql.substring(0, sql.indexOf(" limit "));
			}
			sqlTem += " limit " + start + "," + rowNum2;
			return sqlTem;
		}
		
		
	}

}
