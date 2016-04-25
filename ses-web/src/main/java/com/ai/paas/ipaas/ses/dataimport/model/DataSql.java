package com.ai.paas.ipaas.ses.dataimport.model;

import java.io.Serializable;
import java.util.List;

/**
 * 配置sql
 *
 */
public class DataSql implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 933527941994846621L;
	
	/**主键*/
	private int id;
	/**主表*/
	private transient PrimarySql primarySql;
	/**辅助表*/
	private transient List<FiledSql> filedSqls;
	
	
	public PrimarySql getPrimarySql() {
		return primarySql;
	}
	public void setPrimarySql(PrimarySql primarySql) {
		this.primarySql = primarySql;
	}
	public List<FiledSql> getFiledSqls() {
		return filedSqls;
	}
	public void setFiledSqls(List<FiledSql> filedSqls) {
		this.filedSqls = filedSqls;
	}
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	
	
	
	

}
