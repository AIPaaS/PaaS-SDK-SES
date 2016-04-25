package com.ai.paas.ipaas.ses.dataimport.dbs.config;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ai.paas.ipaas.ses.dataimport.model.DataBaseAttr;
import com.ai.paas.ipaas.ses.dataimport.model.DataSql;
import com.ai.paas.ipaas.ses.dataimport.model.FiledSql;
import com.ai.paas.ipaas.ses.dataimport.model.LineMapParam;

/**
 * Export format config
 * Created by gaoht on 15/6/9.
 */
public class ExportFormatConfig extends FormatConfig{

    private String[] tables;

    private String targetPath;
    
    //liwx3 add
    private DataSql dataSql;
    private List<DataBaseAttr> dataBaseAttrs;
    
    //alias--db
    private Map<String,DataBaseAttr> db;
    //alias--sql
    private Map<String,FiledSql> filedSql;
    //palias.filed--filesSql.alias
//    private Map<String,String> sqlRalitions;
    //palias.filed--indexSql.alias
//    private Map<String,String> indexSqlRalitions;
    
    
    
    //a.f--filedsql
    private Map<String,List<FiledSql>> afIndexFiledSqls;
    //pa.f--a.f
//    private Map<String,List<String>> pafIndexAfs;
    
    //a.f--filedsql （roles.roleId -- memuSql）
    private Map<String,List<FiledSql>> afFiledSqls;
    //pa.f--a.f
    private Map<String,List<String>> pafAfs;
    

    //a.b as c   -->  a.c--b    2015-11-17 
    private Map<String,String> tableFileds;
    
    
    //mapping中的Object
    private List<LineMapParam> lineMapParams;
    private boolean hasObj;
    
    private String sesUserInfo;
    private int type;
    
    

    public String[] getTables() {
        return tables;
    }

    public void setTables(String tables) {
        this.tables = tables.split(",");
    }

    public String getTargetPath() {
        return targetPath;
    }

    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

	public DataSql getDataSql() {
		return dataSql;
	}

	public void setDataSql(DataSql dataSql) {
		this.dataSql = dataSql;
	}

	public List<DataBaseAttr> getDataBaseAttrs() {
		return dataBaseAttrs;
	}

	public void setDataBaseAttrs(List<DataBaseAttr> dataBaseAttrs) {
		this.dataBaseAttrs = dataBaseAttrs;
	}

	public Map<String, DataBaseAttr> getDb() {
		return db;
	}

	public void setDb(Map<String, DataBaseAttr> db) {
		this.db = db;
	}

	public Map<String, FiledSql> getFiledSql() {
		return filedSql;
	}

	public void setFiledSql(Map<String, FiledSql> filedSql) {
		this.filedSql = filedSql;
	}


	public Map<String, List<FiledSql>> getAfFiledSqls() {
		return afFiledSqls;
	}

	public void setAfFiledSqls(Map<String, List<FiledSql>> afFiledSqls) {
		this.afFiledSqls = afFiledSqls;
	}

	public Map<String, List<String>> getPafAfs() {
		return pafAfs;
	}

	public void setPafAfs(Map<String, List<String>> pafAfs) {
		this.pafAfs = pafAfs;
	}

	public Map<String, List<FiledSql>> getAfIndexFiledSqls() {
		return afIndexFiledSqls;
	}

	public void setAfIndexFiledSqls(Map<String, List<FiledSql>> afIndexFiledSqls) {
		this.afIndexFiledSqls = afIndexFiledSqls;
	}
	
	public Map<String, String> getTableFileds() {
		return tableFileds;
	}

	public void setTableFileds(Map<String, String> tableFileds) {
		this.tableFileds = tableFileds;
	}

	public String getSesUserInfo() {
		return sesUserInfo;
	}

	public void setSesUserInfo(String sesUserInfo) {
		this.sesUserInfo = sesUserInfo;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public List<LineMapParam> getLineMapParams() {
		return lineMapParams;
	}

	public void setLineMapParams(List<LineMapParam> lineMapParams) {
		this.lineMapParams = lineMapParams;
	}

	public boolean isHasObj() {
		if(lineMapParams==null||lineMapParams.isEmpty())
			return false;
		return true;
	}

	public void setHasObj(boolean hasObj) {
		this.hasObj = hasObj;
	}

    
    
}
