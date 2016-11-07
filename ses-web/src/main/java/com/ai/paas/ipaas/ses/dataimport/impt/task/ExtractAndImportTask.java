package com.ai.paas.ipaas.ses.dataimport.impt.task;

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

import com.ai.paas.ipaas.search.ISearchClient;
import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.ConnectionFactory;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.ExportFormatConfig;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.TableRuleConfig;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.TableRuleConfig.LogicDb;
import com.ai.paas.ipaas.ses.dataimport.impt.model.Result;
import com.ai.paas.ipaas.ses.dataimport.model.LineMapParam;
import com.ai.paas.ipaas.ses.dataimport.util.GsonUtil;
import com.ai.paas.ipaas.ses.dataimport.util.JdbcUtil;
import com.ai.paas.ipaas.ses.dataimport.util.ParamUtil;
import com.ai.paas.ipaas.txs.dtm.TransactionContext;
import com.ai.paas.ipaas.vo.ses.SesDataSourceInfo;
import com.ai.paas.ipaas.vo.ses.SesIndexFiledSql;

/**
 * PairTask调度 去读主表 拼装fileds toEs
 *
 */
public class ExtractAndImportTask implements Runnable {
	private static final Logger log = LoggerFactory
			.getLogger(ExtractAndImportTask.class);

	private TableRuleConfig.LogicDb logicDb;
	private ExportFormatConfig ec;
	private TableRuleConfig.TablePair tablePair;

	private Result result;
	private CountDownLatch exImLatch = null;
	private ISearchClient is;
	private SimpleDateFormat dateFormat_hms = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ssZZZ");

	private int count = 0;

	public ExtractAndImportTask(final TableRuleConfig.LogicDb logicDb,
			final ExportFormatConfig ec,
			final TableRuleConfig.TablePair tablePair,
			CountDownLatch exImLatch, Result result, ISearchClient is) {
		this.logicDb = logicDb;
		this.ec = ec;
		this.tablePair = tablePair;
		this.exImLatch = exImLatch;
		this.result = result;
		this.is = is;
	}

	@Override
	public void run() {
		try {
			if (ec.getType() == SesDataImportConstants.COMMON_DB_TYPE) {
				readData();
			} else {
				readDbFromDBS(logicDb, ec, tablePair);
			}
		} catch (Exception e) {
			log.error("execute exception ", e);
		}
	}

	/**
	 * 普通数据库
	 */
	private void readData() {
		Connection conn = null;
		PreparedStatement statement = null;
		String sql = null;
		String oSql = null;
		ResultSet rs = null;
		int totalNum = 0;
		try {
			SesDataSourceInfo attr = ec.getDb().get(
					ec.getDataSql().getPrimarySql().getDrAlias());
			conn = JdbcUtil.getConnection(attr);
			if (conn == null) {
				throw new Exception("Connection is null.");
			}
			log.debug("Read Primary begin ");
			oSql = ec.getDataSql().getPrimarySql().getSql();
			sql = oSql.toLowerCase();
			String totalSql = getTotalSql(oSql, sql);
			log.debug("-------totalSql----{}------", totalSql);
			statement = conn.prepareStatement(totalSql);
			if (statement == null) {
				throw new Exception("statement is null.");
			}
			rs = statement.executeQuery();
			rs.next();
			totalNum = rs.getInt(1);
		} catch (Exception e) {
			result.addExcLogs("total number error," + e.getMessage());
			log.error("--------------db exception:" + e.getMessage(), e);
			try {
				Thread.sleep(1001);
			} catch (InterruptedException i) {
				log.error("--------------db exception--", i);
			}
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				log.error(
						"--------------db close resultSet exception:"
								+ e.getMessage(), e);
			}
			try {
				if (statement != null)
					statement.close();
			} catch (SQLException e) {
				log.error("--------------db close preparedStatement exception:"
						+ e.getMessage(), e);
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				log.error("--------------db close preparedStatement exception:"
						+ e.getMessage(), e);
			}
		}
		result.addTotal(totalNum);
		if (totalNum > 0) {
			String numSize = getNumSize(totalNum);
			int threadNum = Integer.valueOf(numSize.split(",")[0]).intValue();
			int pageSize = Integer.valueOf(numSize.split(",")[1]).intValue();

			log.debug(" totalNum {},threadNum {},pageSize {}", totalNum,
					threadNum, pageSize);

			ExecutorService executorService = new ThreadPoolExecutor(0,
					threadNum, 10L, TimeUnit.SECONDS,
					new SynchronousQueue<Runnable>());
			final CountDownLatch taskLatch = new CountDownLatch(threadNum);

			for (int i = 0; i < threadNum; i++) {
				executorService.execute(new ExtractImTask(totalNum, threadNum,
						pageSize, i, sql, oSql, taskLatch));
			}
			log.debug("wait for ExtractImTask thread finished");
			try {
				taskLatch.await();
			} catch (InterruptedException ignored) {
				log.error("taskLatch.await", ignored);
			}
			executorService.shutdown();
			while (!executorService.isTerminated()) {
				try {
					executorService.awaitTermination(10, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					log.error("ExtractImTask InterruptedException", e);
				}
			}
			log.debug("ExtractImTask thread finished task");
		}
		if (exImLatch != null)
			exImLatch.countDown();

	}

	private void readDbFromDBS(final LogicDb logicDb,
			final ExportFormatConfig ec,
			final TableRuleConfig.TablePair tablePair) throws Exception {

		Connection conn = null;
		PreparedStatement statement = null;
		String sql = null;
		ResultSet rs = null;
		int totalNum = 0;
		try {
			conn = ConnectionFactory.getConn(logicDb.masterUrl);

			log.debug("Read table :{}", tablePair);
			sql = ec.getDataSql().getPrimarySql().getSql()
					.replace(ec.getTables()[0], "{0}");
			sql = ParamUtil.fillStringByArgs(sql,
					new String[] { tablePair.realTableName });
			String totalSql = getTotalSql(sql, sql.toLowerCase());
			if (conn != null)
				statement = conn.prepareStatement(totalSql);
			if (conn == null || statement == null) {
				throw new Exception("Connection is null.");
			}
			rs = statement.executeQuery(totalSql);
			rs.next();
			totalNum = rs.getInt(1);

		} catch (Exception e) {
			result.addExcLogs("total number error," + e.getMessage());
			log.error("--------------db exception:" + e.getMessage(), e);
			try {
				Thread.sleep(1001);
			} catch (InterruptedException i) {
				log.error("--------------db exception--", i);
			}
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (SQLException e) {
				log.error(
						"--------------db close resultSet exception:"
								+ e.getMessage(), e);
			}
			try {
				if (statement != null)
					statement.close();
			} catch (SQLException e) {
				log.error("--------------db close preparedStatement exception:"
						+ e.getMessage(), e);
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				log.error("--------------db close preparedStatement exception:"
						+ e.getMessage(), e);
			}
		}
		// totalNum = 3003;
		result.addTotal(totalNum);
		String numSize = getNumSize(totalNum);
		int threadNum = Integer.valueOf(numSize.split(",")[0]).intValue();
		int pageSize = Integer.valueOf(numSize.split(",")[1]).intValue();

		log.debug(" totalNum {},threadNum {},pageSize {}", totalNum, threadNum,
				pageSize);

		ExecutorService executorService = new ThreadPoolExecutor(0, threadNum,
				10L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
		final CountDownLatch taskLatch = new CountDownLatch(threadNum);

		for (int i = 0; i < threadNum; i++) {
			executorService.execute(new ExtractImTask(totalNum, threadNum,
					pageSize, i, sql.toLowerCase(), sql, taskLatch));
		}
		log.debug("wait for ExtractImTask thread finished");
		try {
			taskLatch.await();
		} catch (InterruptedException ignored) {
			log.error("taskLatch.await", ignored);
		}
		executorService.shutdown();
		while (!executorService.isTerminated()) {
			try {
				executorService.awaitTermination(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				log.error("ExtractImTask InterruptedException", e);
			}
		}
		log.debug("ExtractImTask thread finished task");
		if (exImLatch != null)
			exImLatch.countDown();
	}

	private String getNumSize(int totalNum) {
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

	private String getTotalSql(String oSql, String sql) {
		int start = sql.indexOf("select ") + "select ".length();
		int end = sql.indexOf(" from");
		String filed = oSql.substring(start, end).trim();
		return oSql.replace(filed, " count(*) as totalNum ");
	}

	private class ExtractImTask implements Runnable {
		private int threadNum;
		private int pageSize;
		private int num;
		private int totalNum;
		private String sql;
		private String oSql;
		private CountDownLatch taskLatch;

		@SuppressWarnings("unused")
		private ExtractImTask() {
		}

		public ExtractImTask(int totalNum, int threadNum, int pageSize,
				int num, String sql, String oSql, CountDownLatch taskLatch) {
			this.threadNum = threadNum;
			this.pageSize = pageSize;
			this.num = num;
			this.sql = sql;
			this.oSql = oSql;
			this.totalNum = totalNum;
			this.taskLatch = taskLatch;
		}

		@Override
		public void run() {
			try {
				excute(totalNum, threadNum, pageSize, num, sql, oSql);
			} catch (Exception e) {
				result.addExcLogs(e.getMessage());
				log.error(
						"--------------ExtractImTask excute exception:"
								+ e.getMessage(), e);
			}
			taskLatch.countDown();
		}

		public void excute(int totalNum, int threadNum, int rowNum, int i,
				String sql, String oSql) throws Exception {
			Connection conn = null;
			PreparedStatement statement = null;
			ResultSet rs = null;
			try {
				SesDataSourceInfo attr = ec.getDb().get(
						ec.getDataSql().getPrimarySql().getDrAlias());
				if (ec.getType() == SesDataImportConstants.COMMON_DB_TYPE) {
					conn = JdbcUtil.getConnection(attr);
				} else {
					conn = ConnectionFactory.getConn(logicDb.masterUrl);
				}
				Map<String, Object> datas = new HashMap<String, Object>();
				List<String> values = new ArrayList<String>();
				if (conn != null) {
					String preSql = getSql(attr, rowNum, threadNum, totalNum,
							i, sql, oSql);
					log.debug("-------preSql----{}------", preSql);
					statement = conn.prepareStatement(preSql);
				}
				if (conn == null || statement == null) {
					throw new Exception("Connection is null.");
				}

				// 主表结果集
				rs = statement.executeQuery();
				try {
					while (rs.next()) {
						datas.clear();

						for (int j = 0; j < rs.getMetaData().getColumnCount(); j++) {
							String cn = rs.getMetaData().getColumnLabel(j + 1)
									.toLowerCase();
							String cv = "";
							Object obj = rs.getObject(j + 1);
							if (obj instanceof java.util.Date
									|| obj instanceof java.sql.Date
									|| obj instanceof java.sql.Timestamp) {
								cv = dateFormat_hms.format(obj);
							} else {
								cv = rs.getString(j + 1);
							}

							// 辅助字段 2015-11-17
							String af = ec.getDataSql().getPrimarySql()
									.getAlias()
									+ "." + cn;

							if (ec.getAfFiledSqls().containsKey(af)) {
								List<SesIndexFiledSql> fss = ec
										.getAfFiledSqls().get(af);
								for (SesIndexFiledSql fs : fss) {
									log.debug(
											"---------pri-------cv--{}-------",
											cv);
									if (fs.isMapObj()) {
										getFiledValue(
												datas,
												fs,
												cn,
												cv,
												af,
												getlineBean(af, fs.getAlias(),
														true, false));
									} else {
										getFiledValue(datas, fs, cn, cv, af,
												null);
									}
								}
							}
							if(!"rnum".equalsIgnoreCase(cn)) {
								datas.put(cn, cv);
							}
						}
						log.debug("-----excute-------------datas-{}------",
								datas);

						packDatas(datas, ec.getLineMapParams());
						log.debug(
								"-----excute------packDatas-------datas-{}------",
								datas);

						values.add(GsonUtil.objToGson(datas));
						if (values.size() >= 1024) {
							is.bulkJsonInsert(values);
							result.addSucTotal(values.size());
							values.clear();
						}
					}
					if (!values.isEmpty()) {
						is.bulkJsonInsert(values);
						result.addSucTotal(values.size());
						values.clear();
					}
				} catch (Exception e) {
					log.error(
							"--------------get ResultSet exception:"
									+ e.getMessage(), e);
					try {
						result.addExcLogs(e.getMessage());
						Thread.sleep(1001);
					} catch (Exception ee) {
						log.error(
								"--------------addExcLogs exception:"
										+ ee.getMessage(), ee);
					}
				}

			} catch (Exception e) {
				log.error("-------------- exception:" + e.getMessage(), e);
				throw e;
			} finally {
				try {
					if (rs != null)
						rs.close();
				} catch (SQLException e) {
					log.error(
							"--------------db close resultSet exception:"
									+ e.getMessage(), e);
				}
				try {
					if (statement != null)
						statement.close();
				} catch (SQLException e) {
					log.error(
							"--------------db close preparedStatement exception:"
									+ e.getMessage(), e);
				}
				try {
					if (conn != null)
						conn.close();
				} catch (SQLException e) {
					log.error(
							"--------------db close preparedStatement exception:"
									+ e.getMessage(), e);
				}
			}

		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		private void packDatas(Map<String, Object> datas,
				List<LineMapParam> lineMapParams) {
			if (lineMapParams != null && lineMapParams.size() > 0) {
				for (LineMapParam l : lineMapParams) {
					if (l.getPaf() == null || "".equals(l.getPaf())) {
						Object obj = l.getRs();
						if (l.isMany() && (obj != null)
								&& obj instanceof ArrayList) {
							ArrayList flists = (ArrayList) obj;
							String af = l.getAf();
							String parentAlias = l.getAlias();
							ArrayList parentflists = flists;
							while (getChildFiled(af, parentAlias) != null) {
								LineMapParam child = getChildFiled(af,
										parentAlias);
								ArrayList childList = null;
								if (child.isMany()
										&& child.getRs() instanceof ArrayList) {
									childList = (ArrayList) child.getRs();
									int counts = 0;
									if (parentflists != null
											&& parentflists.size() > 0) {
										for (int i = 0; i < parentflists.size(); i++) {
											HashMap flistMap = (HashMap) parentflists
													.get(i);
											List<Map> fileds = new ArrayList<Map>();
											for (; counts < childList.size(); counts++) {

												if (((HashMap) childList
														.get(counts))
														.containsKey("flagBreak")) {
													if (counts == 0)
														continue;
													counts++;
													break;
												}
												fileds.add((HashMap) childList
														.get(counts));
											}
											flistMap.put(getLocalFiled(child
													.getAlias()), fileds);

										}
									}
								} else if (!child.isMany()
										&& child.getRs() instanceof HashMap) {
									HashMap childmap = (HashMap) child.getRs();
									if (parentflists != null
											&& parentflists.size() > 0)
										((HashMap) parentflists.get(0))
												.put(getLocalFiled(child
														.getAlias()), childmap);
								}
								af = child.getAf();
								parentAlias = child.getAf();
								parentflists = childList;
							}
							datas.put(l.getAlias(), flists);

						} else if (!l.isMany() && (obj != null)
								&& obj instanceof HashMap) {
							HashMap map = (HashMap) obj;
							String af = l.getAf();
							String parentAlias = l.getAlias();

							Map parentMap = map;
							while (getChildFiled(af, parentAlias) != null) {
								LineMapParam child = getChildFiled(af,
										parentAlias);
								if (child.isMany()
										&& child.getRs() instanceof ArrayList) {
									ArrayList childlist = (ArrayList) child
											.getRs();
									int counts = 0;
									List<Map> fileds = new ArrayList<Map>();
									for (; counts < childlist.size(); counts++) {
										if (((HashMap) childlist.get(counts))
												.containsKey("flagBreak")) {
											if (counts == 0)
												continue;
											counts++;
											break;
										}
										fileds.add((HashMap) childlist
												.get(counts));
									}
									parentMap.put(
											getLocalFiled(child.getAlias()),
											fileds);
								} else if (!child.isMany()
										&& child.getRs() instanceof HashMap) {
									HashMap childmap = (HashMap) child.getRs();
									parentMap.put(
											getLocalFiled(child.getAlias()),
											childmap);

									parentMap = childmap;
								}

								af = child.getAf();
								parentAlias = child.getAlias();
							}
							datas.put(l.getAlias(), map);

						}
					}

					log.debug("-----LineMapParam---{}-", l.getRs());
				}
			}

		}

		private LineMapParam getChildFiled(String af, String alias) {
			for (LineMapParam l : ec.getLineMapParams()) {
				if (af.equals(l.getPaf()) && l.getAlias().startsWith(alias)) {
					return l;
				}
			}
			return null;
		}

		private LineMapParam getlineBean(String af, String alias,
				boolean start, boolean newBreak) {
			LineMapParam param = null;
			List<LineMapParam> lmps = ec.getLineMapParams();
			if (lmps != null && lmps.size() > 0) {
				for (LineMapParam l : lmps) {
					if (af.equals(l.getAf()) && alias.equals(l.getAlias())) {
						Object value = null;
						if (l.isMany()) {
							if (start) {
								value = new ArrayList<Map<String, String>>();
								l.setCv(null);
							}
						} else {
							value = new HashMap<String, String>();
							l.setCv(null);
						}
						if (newBreak)
							l.setCv("flagBreakStr");
						if (value != null)
							l.setRs(value);
						return l;
					}
				}
			}
			return param;

		}

		private String getSql(SesDataSourceInfo db, int rowNum, int threadNum,
				int totalNum, int i, String sql, String oSql) {
			int start = i * rowNum;
			int rowNum2 = rowNum;
			if (i == threadNum - 1) {
				rowNum2 = totalNum - i * rowNum;
			}
			String sqlTem = sql;
			if (sql.contains(" limit ")) {
				sqlTem = oSql.substring(0, sql.indexOf(" limit "));
			}
			if (db.getDatabase() == SesDataImportConstants.MYSQL_DB) {
				sqlTem += " limit " + start + "," + rowNum2;
			}
			if (db.getDatabase() == SesDataImportConstants.ORACLE_DB) {
				// 重新写
				sqlTem = "SELECT *  FROM (SELECT ROWNUM rnum"
						+ "				              ,a.*				          FROM (" + sqlTem
						+ ") a" + " WHERE ROWNUM <= " + rowNum2 + ")"
						+ " WHERE rnum >" + start;
			}
			// limit
			return sqlTem;
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		private void getFiledValue(Map datas, SesIndexFiledSql fs, String cn,
				String cv, String af, LineMapParam line) throws Exception {
			log.debug("-----getFiledValue-------------count-{}---af {}---",
					count++, af);

			SesDataSourceInfo attr = ec.getDb().get(fs.getDrAlias());
			Connection conn = null;
			PreparedStatement preparedStatement = null;
			ResultSet resultSet = null;

			Map<String, List<String>> fvalues = null;
			boolean many = false;
			if (fs.getRelation() == SesDataImportConstants.ONE_TO_MANY_RELATION) {
				many = true;
				fvalues = new HashMap<String, List<String>>();
			}

			Map<String, String> fvalue4line = null;
			boolean nextNewLine = true;

			try {
				if (SesDataImportConstants.DBS_DB_TYPE == attr.getType()
						&& attr.isHaveTXS())
					TransactionContext.initVisualContext();
				conn = JdbcUtil.getConnection(attr);
				String temp = fs.getSql().toLowerCase().replace(af, "?");
				log.debug("-------getFiledValue sql----{}---cv-{}--", temp, cv);
				preparedStatement = conn.prepareStatement(temp);
				preparedStatement.setString(1, cv);

				resultSet = preparedStatement.executeQuery();
				int tempCount = 0;
				Object obj=null;
				while (resultSet.next()) {
					if (line != null && line.isMany()) {
						fvalue4line = new HashMap<String, String>();
						if (tempCount > 0)
							nextNewLine = false;
					} else {
						nextNewLine = true;
					}
					for (int i = 0; i < resultSet.getMetaData()
							.getColumnCount(); i++) {
						String rcn = resultSet.getMetaData()
								.getColumnLabel(i + 1).toLowerCase();
						obj=null;
						obj=resultSet.getObject(i + 1);
						String rcv=null;
						if (obj instanceof java.util.Date
								|| obj instanceof java.sql.Date
								|| obj instanceof java.sql.Timestamp) {
							rcv = dateFormat_hms.format(obj);
						} else {
							rcv = resultSet.getString(i + 1);
						}
						if (many) {
							if (fs.isMapObj() && line != null && line.isMany()) {
								fvalue4line.put(rcn, rcv);
							} else {
								if (fvalues.containsKey(rcn)) {
									fvalues.get(rcn).add(rcv);
								} else {
									List<String> value = new ArrayList<String>();
									value.add(rcv);
									fvalues.put(rcn, value);
								}
								datas.put(rcn, fvalues.get(rcn));
							}
						} else {
							if (!fs.isMapObj())
								datas.put(resultSet.getMetaData()
										.getColumnLabel(i + 1).toLowerCase(), rcv);
							if (line != null)
								line.put(resultSet.getMetaData()
										.getColumnLabel(i + 1).toLowerCase(), rcv);

						}
						// 2015-11-17
						final String saf = fs.getAlias() + "." + rcn;

						if (fs.isMapObj() && line != null) {
							if (ec.getAfFiledSqls().containsKey(saf)) {
								List<SesIndexFiledSql> fss = ec
										.getAfFiledSqls().get(saf);
								for (SesIndexFiledSql ffs : fss) {
									tempCount++;
									log.debug(
											"---------file-------rcv--{}-----saf--{}----af--{}"
													+ "--nextNewLine-{}--",
											rcv, saf, af, nextNewLine);
									// getFiledValue(datas,ffs,rcn,rcv,saf,getlineBean(saf,ffs.getAlias(),nextNewLine,false));
									getFiledValue(
											datas,
											ffs,
											rcn,
											rcv,
											saf,
											getlineBean(saf, ffs.getAlias(),
													nextNewLine, true));
								}
							}
						} else {
							if (ec.getPafAfs().containsKey(saf)) {
								List<String> afs = ec.getPafAfs().get(saf);
								for (String tempAf : afs) {
									getFiledValue(
											datas,
											ec.getFiledSql().get(
													tempAf.substring(0, tempAf
															.lastIndexOf("."))),
											rcn, rcv, saf, null);
								}
							}
						}
					}
					if (line != null && line.isMany()) {
						log.debug("-----LineMapParam-line--cv-{}--", cv);
						// cv变化时加上标识
						line.add(fvalue4line, cv);
					}

				}

				if (SesDataImportConstants.DBS_DB_TYPE == attr.getType()
						&& attr.isHaveTXS())
					TransactionContext.clear();
			} catch (Exception e) {
				// result.addExcLogs(e.getMessage());
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
					log.error(
							"--------------db close preparedStatement exception:"
									+ e.getMessage(), e);
				}
				JdbcUtil.closeConnection(conn);
			}

		}

		// 获得本地字段
		private String getLocalFiled(String alias) {
			return alias.contains(".") ? alias
					.substring(alias.lastIndexOf(".") + 1) : alias;
		}

		@SuppressWarnings({ "unused", "rawtypes" })
		private void getFiledValueUseIndex(Map datas, SesIndexFiledSql fs,
				String cn, String cv, String af) throws Exception {
			SesDataSourceInfo attr = ec.getDb().get(fs.getDrAlias());
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
						.toLowerCase().replace(af, "?"));
				preparedStatement.setString(1, cv);
				resultSet = preparedStatement.executeQuery();
				Object obj=null;
				while (resultSet.next()) {
					for (int i = 0; i < resultSet.getMetaData()
							.getColumnCount(); i++) {
						String rcn = resultSet.getMetaData()
								.getColumnLabel(i + 1).toLowerCase();
						// String rcn = resultSet.getMetaData().getColumnName(i
						// + 1)
						// .toLowerCase();
						obj=null;
						obj=resultSet.getObject(i + 1);
						String rcv=null;
						if (obj instanceof java.util.Date
								|| obj instanceof java.sql.Date
								|| obj instanceof java.sql.Timestamp) {
							rcv = dateFormat_hms.format(obj);
						} else {
							rcv = resultSet.getString(i + 1);
						}
						String sql = fs.getSql().toLowerCase();
						if (ec.getTableFileds().containsKey(
								fs.getIndexAlias() + "." + rcn)) {

						} else {

						}
						// 2015-11-17
						final String saf = fs.getIndexAlias() + "." + rcn;
						// final String saf =
						// ec.getTableFileds().containsKey(fs.getIndexAlias() +
						// "."+rcn)?
						// (fs.getIndexAlias() + "."
						// +ec.getTableFileds().get(fs.getIndexAlias() +
						// "."+rcn)):
						// fs.getIndexAlias() + "." + rcn;
						if (sql.contains(saf)) {
							getFiledValue(datas, fs, rcn, rcv, saf, null);
						}

					}
				}
				if (attr.getType() == SesDataImportConstants.DBS_DB_TYPE)
					TransactionContext.clear();
			} catch (Exception e) {
				try {
					// result.addExcLogs(e.getMessage());
					JdbcUtil.removeConnection(attr.getId());
				} catch (SQLException e1) {
					log.error("--------------db exception:" + e1.getMessage(),
							e1);
				}
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
					log.error(
							"--------------db close preparedStatement exception:"
									+ e.getMessage(), e);
				}
				try {
					JdbcUtil.closeConnection(conn);
				} catch (SQLException e1) {
					log.error("--------------db exception:" + e1.getMessage(),
							e1);
				}

			}

		}

	}
}
