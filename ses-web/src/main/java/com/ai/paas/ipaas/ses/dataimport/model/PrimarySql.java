package com.ai.paas.ipaas.ses.dataimport.model;

import java.io.Serializable;

/**
 * 主表，sql
 *
 */
public class PrimarySql implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -6333000303844693951L;
	
	/**是否主表*/
	private boolean isPrimary;
	/**别名*/
	private String alias;
	/**数据源别名*/
	private String drAlias;
	/**主键*/
	private String primaryKey;
	private String sql;
	public boolean isPrimary() {
		return isPrimary;
	}
	public void setPrimary(boolean isPrimary) {
		this.isPrimary = isPrimary;
	}
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	public String getDrAlias() {
		return drAlias;
	}
	public void setDrAlias(String drAlias) {
		this.drAlias = drAlias;
	}
	public String getPrimaryKey() {
		return primaryKey;
	}
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	
	
	
	
	
}
