package com.ai.paas.ipaas.ses.dataimport.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.dbs.distribute.DistributedDataSource;
import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.txs.dtm.TransactionContext;
import com.ai.paas.ipaas.vo.ses.SesDataSourceInfo;

public class JdbcUtil {
	private static final transient Logger log = LoggerFactory
			.getLogger(JdbcUtil.class);

	private static Map<Integer, Connection> connections = new HashMap<Integer, Connection>();
	private static Map<Integer, DataSource> dataSources = new HashMap<Integer, DataSource>();

	private JdbcUtil() {

	}

	/**
	 * 数据库是否存活
	 * 
	 * @param id
	 * @return
	 * @throws Exception
	 */
	public static boolean isAlived(SesDataSourceInfo attr) throws Exception {
		Connection con = initDbCon(attr);
		return isAlived(con, attr, null);
	}

	/**
	 * validate sql
	 * 
	 * @param id
	 * @return
	 * @throws Exception
	 */
	public static boolean validateSql(SesDataSourceInfo attr, String sql)
			throws Exception {
		Connection con = initDbCon(attr);
		return isAlived(con, attr, sql);
	}

	/**
	 * @param key
	 *            id
	 * @return
	 * @throws SQLException
	 */
	private static Connection getConnection(int key) throws SQLException {
		DataSource dataSource = dataSources.get(key);
		if (dataSource == null)
			return null;
		return dataSource.getConnection();
	}

	/**
	 * @param key
	 *            id
	 * @return
	 * @throws Exception
	 */
	public static Connection getConnection(SesDataSourceInfo attr)
			throws Exception {
		DataSource dataSource = dataSources.get(attr.getId());
		if (dataSource == null) {
			initDataSource(attr);
			dataSource = dataSources.get(attr.getId());
		}
		return dataSource.getConnection();
	}

	/**
	 * @param key
	 *            id
	 * @return
	 * @throws SQLException
	 */
	public static void removeConnection(int db) throws SQLException {
		Connection con = getConnection(db);
		if (con != null)
			con.close();
		connections.remove(db);
	}

	/**
	 * @param key
	 *            id
	 * @return
	 * @throws SQLException
	 */
	public static void removeDataSrouce(int db) {
		dataSources.remove(db);
	}

	/**
	 * @param key
	 *            id
	 * @return
	 * @throws SQLException
	 */
	public static void closeConnection(int db) throws SQLException {
		Connection con = getConnection(db);
		if (con != null)
			con.close();
	}

	/**
	 * @param key
	 *            id
	 * @return
	 * @throws SQLException
	 */
	public static void closeConnection(Connection con) throws SQLException {
		if (con != null)
			con.close();
	}

	/**
	 * @param key
	 *            数据库的id
	 */
	private static Connection initDbCon(SesDataSourceInfo attr)
			throws Exception {
		try {
			String url = "";
			Connection connnection = null;
			if (attr.getType() == SesDataImportConstants.COMMON_DB_TYPE) {
				// 建立数据库连接
				String userName = attr.getUsername();
				String pwd = attr.getPwd();
				if (attr.getDatabase() == SesDataImportConstants.MYSQL_DB) {
					url = "jdbc:mysql://" + attr.getIp() + ":" + attr.getPort()
							+ "/" + attr.getSid()
							+ "?useUnicode=true&characterEncoding=UTF-8";
					// The newInstance() call is a work around for some
					// broken Java implementations
					Class.forName("com.mysql.jdbc.Driver");
				}
				if (attr.getDatabase() == SesDataImportConstants.ORACLE_DB) {
					url = "jdbc:oracle:thin:@" + attr.getIp() + ":"
							+ attr.getPort() + ":" + attr.getSid();
					Class.forName("oracle.jdbc.driver.OracleDriver");
				}
				log.info("----------初始化数据源:" + url);

				connnection = DriverManager.getConnection(url, userName, pwd);
			}
			return connnection;
		} catch (Exception e) {
			log.error("--------------建立数据库连接异常:" + e.getMessage(), e);
			throw new Exception(attr.getIp() + "_" + attr.getPort() + "_"
					+ attr.getSid() + " " + e.getMessage(), e);
		}

	}

	/**
	 * @param key
	 *            数据库的id
	 * @throws Exception
	 */
	public static synchronized void initDataSource(SesDataSourceInfo attr)
			throws Exception {
		try {
			if (dataSources.containsKey(attr.getId()))
				return;
			DataSource dataSource = null;
			dataSource = getMyDataSource(attr);
			if (attr.getType() == SesDataImportConstants.DBS_DB_TYPE) {
				dataSource = dataSources.get(attr.getId());
				if (dataSource == null) {
					dataSource = new DistributedDataSource(attr.getUser(),
							attr.getServicePwd(), attr.getServiceId(),
							attr.getAuthAddr());
				}
			}
			dataSources.put(attr.getId(), dataSource);

		} catch (Exception e) {
			log.error("--------------建立数据库连接异常:" + e.getMessage(), e);
			throw new Exception(attr.getUser().split("@")[0] + "_"
					+ attr.getServiceId() + " ERROR:" + e.getMessage(), e);
		}

	}

	private static DataSource getMyDataSource(SesDataSourceInfo attr)
			throws Exception {
		org.apache.tomcat.jdbc.pool.DataSource datasource = null;
		try {
			org.apache.tomcat.jdbc.pool.PoolProperties p = new org.apache.tomcat.jdbc.pool.PoolProperties();
			if (attr.getDatabase() == SesDataImportConstants.MYSQL_DB) {
				p.setUrl("jdbc:mysql://" + attr.getIp() + ":" + attr.getPort()
						+ "/" + attr.getSid());
				p.setDriverClassName("com.mysql.jdbc.Driver");
				p.setValidationQuery("SELECT 1");
			}
			if (attr.getDatabase() == SesDataImportConstants.ORACLE_DB) {
				p.setUrl("jdbc:oracle:thin:@" + attr.getIp() + ":"
						+ attr.getPort() + ":" + attr.getSid());
				p.setDriverClassName("oracle.jdbc.driver.OracleDriver");
				p.setValidationQuery("SELECT 1 FROM DUAL");
			}
			p.setUsername(attr.getUsername());
			p.setPassword(attr.getPwd());
			p.setJmxEnabled(true);
			p.setTestWhileIdle(false);
			p.setTestOnBorrow(true);
			p.setTestOnReturn(false);
			p.setValidationInterval(30000);
			p.setTimeBetweenEvictionRunsMillis(30000);
			p.setMaxActive(200);
			p.setInitialSize(10);
			p.setMaxWait(10000);
			p.setRemoveAbandonedTimeout(60);
			p.setMinEvictableIdleTimeMillis(30000);
			p.setMinIdle(1);
			p.setLogAbandoned(true);
			p.setRemoveAbandoned(true);
			p.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"
					+ "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");
			datasource = new org.apache.tomcat.jdbc.pool.DataSource();
			datasource.setPoolProperties(p);
		} catch (Exception e) {
			log.error("--------------建立数据库连接异常:" + e.getMessage(), e);
			throw new Exception(attr.getIp() + "_" + attr.getPort() + "_"
					+ attr.getSid() + " " + e.getMessage(), e);
		}
		return datasource;
	}

	/**
	 * 数据库是否存活
	 * 
	 * @param con
	 * @return
	 * @throws SQLException
	 */
	private static boolean isAlived(Connection con, SesDataSourceInfo attr,
			String sql) throws Exception {
		if (con == null)
			return false;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			if (sql != null && sql.length() > 0) {
				String tempSql = sql;
				sql = sql.toLowerCase();
				if (sql.contains(" order by "))
					tempSql = tempSql.substring(0, sql.indexOf(" order by "));
				if (sql.contains(" limit "))
					tempSql = tempSql.substring(0, sql.indexOf(" limit "));
				if (attr.getDatabase() == SesDataImportConstants.MYSQL_DB) {
					tempSql += " limit 0,1 ";
				}
				if (attr.getDatabase() == SesDataImportConstants.ORACLE_DB) {
					// oracle 不一样
					tempSql = tempSql.toLowerCase();
					tempSql = tempSql.replace("select ", "select rownum, ");
					// 加上where
					if (tempSql.indexOf(" where ") >= 0) {
						tempSql = tempSql.replace(" where ",
								" where rownum<=1 and ");
					} else {
						// 如果有order by 呢
						if (tempSql.contains(" order by "))
							tempSql = tempSql.replace(" order by ",
									" where rownum<=1  order by ");
						else
							tempSql += " where rownum<=1";
					}
				}
				sql = tempSql;
			} else {
				if (SesDataImportConstants.MYSQL_DB == attr.getDatabase())
					sql = "select now()";
				if (SesDataImportConstants.ORACLE_DB == attr.getDatabase())
					sql = "select 1 from dual";
			}
			log.debug("--------sql--{}----", sql);
			st = con.prepareStatement(sql);
			rs = st.executeQuery();

			Object date = null;
			while (rs.next()) {
				date = rs.getObject(1);
			}
			return date != null;
		} catch (SQLException se) {
			log.error("--------------SQLException:" + se.getMessage(), se);
			throw se;
		} catch (Exception e) {
			log.error("--------------Exception:" + e.getMessage(), e);
			throw e;
		} finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception ignore) {
				log.error(
						"--------------close ResultSet:" + ignore.getMessage(),
						ignore);
				throw ignore;
			}
			try {
				if (st != null)
					st.close();
			} catch (Exception ignore) {
				log.error(
						"--------------close Statement:" + ignore.getMessage(),
						ignore);
				throw ignore;
			}
			try {
				con.close();
			} catch (Exception ignore) {
				log.error("--------------close con:" + ignore.getMessage(),
						ignore);
				throw ignore;
			}
		}
	}

	/**
	 * 验证DBS的可用性
	 * 
	 * @param attr
	 * @return
	 */
	public static boolean dbsIsAlived(SesDataSourceInfo attr) throws Exception {

		final DistributedDataSource ds = new DistributedDataSource(
				attr.getUser(), attr.getServicePwd(), attr.getServiceId(),
				attr.getAuthAddr());
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			String sql = attr.getVsql();
			if (sql != null) {
				if (SesDataImportConstants.DBS_DB_TYPE == attr.getType()
						&& attr.isHaveTXS())
					TransactionContext.initVisualContext();
				conn = ds.getConnection();
				sql = sql.toLowerCase();
				if (sql.contains(" order by "))
					sql = sql.substring(0, sql.indexOf(" order by "));
				if (sql.contains(" and ")) {
					sql += " and 1=2";
				} else if (sql.contains(" where ")) {
					sql += " and 1=2";
				} else {
					sql += " where 1=2";
				}
				preparedStatement = conn.prepareStatement(sql);
				resultSet = preparedStatement.executeQuery();
				if (SesDataImportConstants.DBS_DB_TYPE == attr.getType()
						&& attr.isHaveTXS())
					TransactionContext.clear();
				return true;
			}
		} catch (Exception e) {
			log.error("--------------dbs exception:" + e.getMessage(), e);
			throw e;
		} finally {
			try {
				if (resultSet != null)
					resultSet.close();
			} catch (SQLException e) {
				log.error(
						"--------------dbs close resultSet exception:"
								+ e.getMessage(), e);
			}
			try {
				if (preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				log.error(
						"--------------dbs close preparedStatement exception:"
								+ e.getMessage(), e);
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				log.error(
						"--------------dbs close conn exception:"
								+ e.getMessage(), e);
			}
		}
		return false;
	}

	public static String validateDataSource(
			List<SesDataSourceInfo> dataBaseAttrs, Map<String, String> userInfo) {
		Map<String, String> res = new HashMap<String, String>();

		if (dataBaseAttrs == null || dataBaseAttrs.isEmpty()) {
			return "{\"CODE\":\"999\",\"MSG\":\"datasource is null.\"}";
		}
		if (dataBaseAttrs.size() == 1) {
			try {
				boolean resVa = validateDB(dataBaseAttrs.get(0));
				if (resVa) {
					res.put("CODE", "000");
					res.put("MSG", "connect datasource success.");
				} else {
					res.put("CODE", "999");
					res.put("MSG", "can not connect datasource.");
				}
			} catch (Exception e) {
				log.error("--validateDB exception--:", e);
				res.put("CODE", "999");
				res.put("MSG", e.getMessage());
			}

		}
		return GsonUtil.objToGson(res);
	}

	private static boolean validateDB(SesDataSourceInfo attr) throws Exception {
		if (SesDataImportConstants.COMMON_DB_TYPE == attr.getType())
			return JdbcUtil.isAlived(attr);
		if (SesDataImportConstants.DBS_DB_TYPE == attr.getType())
			return JdbcUtil.dbsIsAlived(attr);
		return false;
	}

	public static String validateSql(List<SesDataSourceInfo> dataSources,
			List<SesDataSourceInfo> oraDataSources,
			Map<String, String> userInfo, HttpServletRequest request) {
		Map<String, String> res = new HashMap<String, String>();

		String groupId = request.getParameter("groupId");
		if (groupId != null && groupId.length() > 0) {
			try {
				boolean resVa = false;
				String sql = "";
				SesDataSourceInfo db = null;
				if (Integer.valueOf(groupId) == SesDataImportConstants.GROUP_ID_1) {
					sql = request.getParameter("sql");
					dataSources = ParamUtil.getDs(request, null);
					if (dataSources == null || dataSources.isEmpty())
						return "{\"CODE\":\"999\",\"MSG\":\"datasrouce is null.\"}";
					db = dataSources.get(0);
				}
				if (Integer.valueOf(groupId) == SesDataImportConstants.GROUP_ID_2) {
					String isPriStr = request.getParameter("isPrimary");
					if (isPriStr != null && isPriStr.length() > 0
							&& Boolean.valueOf(isPriStr)) {
						sql = request.getParameter("sql");
						db = oraDataSources.get(0);
					} else {
					}
				}
				resVa = JdbcUtil.validateSql(db, sql);

				if (resVa) {
					res.put("CODE", "000");
					res.put("MSG", "config sql success.");
				} else {
					res.put("CODE", "999");
					res.put("MSG", "sql is invalite.");
				}
			} catch (Exception e) {
				log.error("--validateDB exception--:", e);
				res.put("CODE", "999");
				res.put("MSG", e.getMessage());
			}
		}
		return GsonUtil.objToGson(res);
	}
}
