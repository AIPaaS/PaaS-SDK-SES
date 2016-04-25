package com.ai.paas.ipaas.ses.dataimport.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.dbs.distribute.DistributedDataSource;
import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.model.DataBaseAttr;
import com.ai.paas.ipaas.txs.dtm.TransactionContext;

public class JdbcUtil {
	private static final transient Logger log = LoggerFactory.getLogger(JdbcUtil.class);


	private static Map<Integer,Connection> connections = new HashMap<Integer,Connection>();
	private static Map<Integer,DataSource> dataSources = new HashMap<Integer,DataSource>();
	
	private static Map<Integer,Integer> dbs = new HashMap<Integer,Integer>();
	
	private JdbcUtil(){
		
	}
	
	/**
	 * 数据库是否存活
	 * @param id
	 * @return
	 * @throws Exception 
	 */
	public static boolean isAlived(DataBaseAttr attr) throws Exception{
		Connection con = initDbCon(attr);
		return isAlived(con,attr,null);
	}
	/**
	 * validate sql
	 * @param id
	 * @return
	 * @throws Exception 
	 */
	public static boolean validateSql(DataBaseAttr attr,String sql) throws Exception{
		Connection con = initDbCon(attr);
		return isAlived(con,attr,sql);
	}
	/**
	 * @param key id
	 * @return
	 * @throws SQLException
	 */
	private static Connection getConnection(int key) throws SQLException{
		DataSource dataSource = dataSources.get(key);
		if(dataSource==null)
			return null;
		return dataSource.getConnection();
	} 
	/**
	 * @param key id
	 * @return
	 * @throws Exception 
	 */
	public static Connection getConnection(DataBaseAttr attr) throws Exception{
		DataSource dataSource = dataSources.get(attr.getId());
		if(dataSource==null){
			initDataSource(attr);
			dataSource = dataSources.get(attr.getId());
		}
		return dataSource.getConnection();
	} 
	/**
	 * @param key id
	 * @return
	 * @throws SQLException
	 */
	public static void removeConnection(int db) throws SQLException{
		Connection con = getConnection(db);
		if(con != null)
			con.close();
		connections.remove(db);
	} 
	/**
	 * @param key id
	 * @return
	 * @throws SQLException
	 */
	public static void removeDataSrouce(int db){
		dataSources.remove(db);
	} 
	/**
	 * @param key id
	 * @return
	 * @throws SQLException
	 */
	public static void closeConnection(int db) throws SQLException{
		Connection con = getConnection(db);
		if(con != null)
			con.close();
	} 
	/**
	 * @param key id
	 * @return
	 * @throws SQLException
	 */
	public static void closeConnection(Connection con) throws SQLException{
		if(con != null)
			con.close();
	} 

	/**
	 * @param key 数据库的id
	 */
	private static Connection initDbCon(DataBaseAttr attr) throws Exception{
		try {  
			String url = "";
			Connection connnection = null;
			if(attr.getType()==SesDataImportConstants.COMMON_DB_TYPE){
	            // 建立数据库连接 
				String userName = attr.getUsername();
				String pwd = attr.getPwd();
				if(attr.getDatabase()==SesDataImportConstants.MYSQL_DB)
					url = "jdbc:mysql://"+attr.getIp()+":"+attr.getPort()+"/"+attr.getSid();
				if(attr.getDatabase()==SesDataImportConstants.ORACLE_DB)
					url = "jdbc:oracle:thin:@"+attr.getIp()+":"+attr.getPort()+":"+attr.getSid();
				log.info("----------初始化数据源:"+url);
	
			 	connnection = DriverManager.getConnection(url, userName,  
			 			pwd);  
		 	}
			return connnection;
        } catch (Exception e) {  
        	log.error("--------------建立数据库连接异常:"+e.getMessage(),e);
        	throw new Exception(attr.getIp()+"_"+attr.getPort()+"_"+attr.getSid()+" "+e.getMessage(),e);
        }
		
	}
	/**
	 * @param key 数据库的id
	 * @throws Exception 
	 */
	public static synchronized void initDataSource(DataBaseAttr attr) throws Exception {
		try {
			if(dataSources.containsKey(attr.getId()))
				return;
			DataSource dataSource = null;
			dataSource = getMyDataSource(attr);
			if(attr.getType()==SesDataImportConstants.DBS_DB_TYPE){
				dataSource = dataSources.get(attr.getId());
				if(dataSource==null){
					dataSource = new DistributedDataSource(
							attr.getUser(), attr.getServicePwd(), attr.getServiceId(),
							attr.getAuthAddr());
				}
			}
			dataSources.put(attr.getId(), dataSource);

        } catch (Exception e) {  
        	log.error("--------------建立数据库连接异常:"+e.getMessage(),e);
        	throw new Exception(attr.getUser().split("@")[0]+"_"+attr.getServiceId()+" ERROR:"+e.getMessage(),e);
        }  
		
	}
	private static DataSource getMyDataSource(DataBaseAttr attr) throws Exception {
		org.apache.tomcat.jdbc.pool.DataSource datasource = null;
		try {
			org.apache.tomcat.jdbc.pool.PoolProperties p = new org.apache.tomcat.jdbc.pool.PoolProperties();
			if(attr.getDatabase()==SesDataImportConstants.MYSQL_DB){
			    p.setUrl("jdbc:mysql://"+attr.getIp()+":"+attr.getPort()+"/"+attr.getSid());
			    p.setDriverClassName("com.mysql.jdbc.Driver");
			    p.setValidationQuery("SELECT 1");
			}
			if(attr.getDatabase()==SesDataImportConstants.ORACLE_DB){
				p.setUrl("jdbc:oracle:thin:@"+attr.getIp()+":"+attr.getPort()+":"+attr.getSid());
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
			p.setJdbcInterceptors(
			  "org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"+
			  "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");
			datasource = new org.apache.tomcat.jdbc.pool.DataSource();
			datasource.setPoolProperties(p);
		} catch (Exception e) {
			log.error("--------------建立数据库连接异常:"+e.getMessage(),e);
        	throw new Exception(attr.getIp()+"_"+attr.getPort()+"_"+attr.getSid()+" "+e.getMessage(),e);
		}
		return datasource;
	}

	/**
	 * 数据库是否存活
	 * @param con
	 * @return
	 * @throws SQLException
	 */
	private static boolean isAlived(Connection con,DataBaseAttr attr,String sql) throws Exception{
		if(con == null)
			return false;
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			if(sql!=null && sql.length()>0){
				String tempSql = sql;
				sql = sql.toLowerCase();
				if(sql.contains(" order by "))
					tempSql = tempSql.substring(0,sql.indexOf(" order by "));
				if(sql.contains(" limit "))
					tempSql = tempSql.substring(0,sql.indexOf(" limit "));
				tempSql += " limit 0,1 ";
				sql = tempSql;
			}else{
				if(SesDataImportConstants.MYSQL_DB == attr.getDatabase())
					sql = "select now()";
				if(SesDataImportConstants.ORACLE_DB == attr.getDatabase())
					sql = "select 1 from dual";
			}
			log.debug("--------sql--{}----",sql);
			st = con.prepareStatement(sql);
			rs = st.executeQuery();

			Object date = null;
			while (rs.next()) {
				date = rs.getObject(1);
			}
			return date != null;
		} catch(SQLException se) {
			log.error("--------------SQLException:"+se.getMessage(),se);
			throw se;
		} catch(Exception e) {
			log.error("--------------Exception:"+e.getMessage(),e);
			throw e;
		}  finally {
			try {
				if (rs != null)
					rs.close();
			} catch (Exception ignore) {
				log.error("--------------close ResultSet:"+ignore.getMessage(),ignore);
				throw ignore;
			}
			try {
				if (st != null)
					st.close();
			} catch (Exception ignore) {
				log.error("--------------close Statement:"+ignore.getMessage(),ignore);
				throw ignore;
			}
			try {
				con.close();
			} catch (Exception ignore) {
				log.error("--------------close con:"+ignore.getMessage(),ignore);
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
	public static boolean dbsIsAlived(DataBaseAttr attr) throws Exception{
		
		final DistributedDataSource ds = new DistributedDataSource(
				attr.getUser(), attr.getServicePwd(), attr.getServiceId(),
				attr.getAuthAddr());
		Connection conn = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		try {
			String sql = attr.getVsql();
			if(sql!=null){
				if (SesDataImportConstants.DBS_DB_TYPE==attr.getType()&&attr.isHaveTXS())
					TransactionContext.initVisualContext();
				conn = ds.getConnection();
				sql = sql.toLowerCase();
				if(sql.contains(" order by "))
					sql = sql.substring(0,sql.indexOf(" order by "));
				if(sql.contains(" and ")){
					sql += " and 1=2";
				}else if(sql.contains(" where ")){
					sql += " and 1=2";
				}else{
					sql += " where 1=2";
				}
				preparedStatement = conn.prepareStatement(sql);
				resultSet = preparedStatement.executeQuery();
				if (SesDataImportConstants.DBS_DB_TYPE==attr.getType()&&attr.isHaveTXS())
					TransactionContext.clear();
				return true;
			}
		} catch (Exception e) {
			log.error("--------------dbs exception:"+e.getMessage(),e);
			throw e;
		} finally {
			try {
				if(resultSet!=null)
					resultSet.close();
			} catch (SQLException e) {
				log.error("--------------dbs close resultSet exception:"+e.getMessage(),e);
			}
			try {
				if(preparedStatement!=null)
					preparedStatement.close();
			} catch (SQLException e) {
				log.error("--------------dbs close preparedStatement exception:"+e.getMessage(),e);
			}
			try {
				if(conn!=null)
					conn.close();
			} catch (SQLException e) {
				log.error("--------------dbs close conn exception:"+e.getMessage(),e);
			}
		}
		return false;
	}
	
	
}
