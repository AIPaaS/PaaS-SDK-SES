package com.ai.paas.ipaas.ses.dataimport.model;

import java.io.Serializable;

/**
 * 辅助sql
 *
 */
public class FiledSql implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5015586402211527083L;
	
	/**别名*/
	private String alias;
	/**数据源别名*/
	private String drAlias;
	/**1 1对1，2 1对多*/
	private int relation;
	/**索引别名*/
	private String indexAlias;
	/**索引sql*/
	private String indexSql;
	/**字段sql*/
	private String sql;
	/**是否对应mapping中的object对象*/
	private boolean mapObj;

	
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
	public int getRelation() {
		return relation;
	}
	public void setRelation(int relation) {
		this.relation = relation;
	}
	public String getIndexAlias() {
		return indexAlias;
	}
	public void setIndexAlias(String indexAlias) {
		this.indexAlias = indexAlias;
	}
	public String getIndexSql() {
		return indexSql;
	}
	public void setIndexSql(String indexSql) {
		this.indexSql = indexSql;
	}
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public boolean isMapObj() {
		return mapObj;
	}
	public void setMapObj(boolean mapObj) {
		this.mapObj = mapObj;
	}
	
	

}
