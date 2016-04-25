package com.ai.paas.ipaas.ses.dataimport.model;

import java.io.Serializable;

public class DataBaseAttr implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 4450280467476241124L;
	
	/**唯一性*/
	private int id;
	
	/**1 单库  2 多库  3 DBS*/
	private int groupId;
	/**数据源 别名*/
	private String alias;
	/**1 普通数据库，2 DBS*/
	private int type;
	
	
	/**云平台账号*/
	private String user;
	/**服务ID*/
	private String serviceId;
	/**服务密码*/
	private String servicePwd;
	/**认证URL*/
	private String authAddr;
	/**验证sql*/
	private String vsql;
	/**是否支持TXS*/
	private boolean haveTXS = true;
	
	
	
	/**1 mysql,2 oracle*/
	private int database;
	/**数据库ip*/
	private String ip;
	/**数据库port*/
	private int port;
	/**数据库sid*/
	private String sid;
	/**数据库用户名*/
	private String username;
	/**数据库密码*/
	private String pwd;
	
	/**ses_dataimport_user.id*/
	private int uId;
	
	private boolean overwrite=false;
	
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getServiceId() {
		return serviceId;
	}
	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}
	public String getServicePwd() {
		return servicePwd;
	}
	public void setServicePwd(String servicePwd) {
		this.servicePwd = servicePwd;
	}
	public int getDatabase() {
		return database;
	}
	public void setDatabase(int database) {
		this.database = database;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String getSid() {
		return sid;
	}
	public void setSid(String sid) {
		this.sid = sid;
	}
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		this.username = username;
	}
	public String getPwd() {
		return pwd;
	}
	public void setPwd(String pwd) {
		this.pwd = pwd;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getVsql() {
		return vsql;
	}
	public void setVsql(String vsql) {
		this.vsql = vsql;
	}
	public String getAuthAddr() {
		return authAddr;
	}
	public void setAuthAddr(String authAddr) {
		this.authAddr = authAddr;
	}
	public boolean isHaveTXS() {
		return haveTXS;
	}
	public void setHaveTXS(boolean haveTXS) {
		this.haveTXS = haveTXS;
	}
	public int getGroupId() {
		return groupId;
	}
	public void setGroupId(int groupId) {
		this.groupId = groupId;
	}
	public int getuId() {
		return uId;
	}
	public void setuId(int uId) {
		this.uId = uId;
	}
	public boolean isOverwrite() {
		return overwrite;
	}
	public void setOverwrite(boolean overwrite) {
		this.overwrite = overwrite;
	}
	
	
}
