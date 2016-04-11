package com.ai.paas.ipaas.ses.dataimport.dbs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.ExportFormatConfig;
import com.ai.paas.ipaas.ses.dataimport.dbs.config.TableRuleConfig;
import com.ai.paas.ipaas.ses.dataimport.dbs.service.TaskService;
import com.ai.paas.ipaas.ses.dataimport.impt.model.Result;
import com.ai.paas.ipaas.ses.dataimport.model.DataBaseAttr;
import com.ai.paas.ipaas.ses.dataimport.model.DataSql;
import com.ai.paas.ipaas.ses.dataimport.model.FiledSql;
import com.ai.paas.ipaas.ses.dataimport.model.PrimarySql;
import com.ai.paas.ipaas.ses.dataimport.util.JdbcUtil;

public class ExportData2ES {
	private static final Logger log = LoggerFactory.getLogger(ExportData2ES.class);
    private final TableRuleConfig config;
    private final ExportFormatConfig exportFormatConfig;
    public ExportData2ES(ExportFormatConfig exportFormatConfig) {
        this.config = new TableRuleConfig(exportFormatConfig.getUserName(), exportFormatConfig.getPassword()
                , exportFormatConfig.getServiceId(), exportFormatConfig.getAuthAddress());
        this.exportFormatConfig = exportFormatConfig;
    }

    public ExportData2ES(String userName,String password,String serviceId,
    		String authAddress,DataSql dataSql,List<DataBaseAttr> dataBaseAttrs,
    		String sesUserInfo,int type) {
    	ExportFormatConfig exportFormatConfig = new ExportFormatConfig();
    	exportFormatConfig.setAuthAddress(authAddress);
    	exportFormatConfig.setUserName(userName);
    	exportFormatConfig.setPassword(password);
    	exportFormatConfig.setServiceId(serviceId);
    	exportFormatConfig.setDataBaseAttrs(dataBaseAttrs);
    	exportFormatConfig.setDataSql(dataSql);
    	exportFormatConfig.setSesUserInfo(sesUserInfo);
    	exportFormatConfig.setType(type);
        this.config = new TableRuleConfig(exportFormatConfig.getUserName(), exportFormatConfig.getPassword()
                , exportFormatConfig.getServiceId(), exportFormatConfig.getAuthAddress());
        this.exportFormatConfig = exportFormatConfig;
    }
    
    

    public Result start() throws Exception {
        String[] tables = getTableName(this.exportFormatConfig.getDataSql().getPrimarySql());
        this.config.init(Arrays.asList(tables));
        initSql();
    	if(exportFormatConfig.getType()==SesDataImportConstants.COMMON_DB_TYPE){
    		return commonDbTask();
    	}else{
    		TaskService service = new TaskService();
    		return service.start(config, exportFormatConfig);
        }

    }

	private Result commonDbTask() throws Exception {
		return null;
	}

	private void initSql() throws Exception {
		if(exportFormatConfig.getDataBaseAttrs()!=null){
			Map<String,DataBaseAttr> dbs = new TreeMap<String,DataBaseAttr>();
			for(DataBaseAttr db: exportFormatConfig.getDataBaseAttrs()){
				dbs.put(db.getAlias(), db);
				JdbcUtil.initDataSource(db);
			}
			exportFormatConfig.setDb(dbs);
		}
		if(exportFormatConfig.getDataSql().getFiledSqls()!=null){
			Map<String,FiledSql> filedSql = new TreeMap<String,FiledSql>();
			for(FiledSql sql: exportFormatConfig.getDataSql().getFiledSqls()){
				filedSql.put(sql.getAlias(), sql);
			}
			exportFormatConfig.setFiledSql(filedSql);
		}
		//palias.filed--filesSql.alias
		Map<String,List<FiledSql>> afIndexFiledSqls = new TreeMap<String,List<FiledSql>>();
		
		
		Map<String,List<FiledSql>> afFiledSqls = new TreeMap<String,List<FiledSql>>();
		Map<String,List<String>> pafAfs = new TreeMap<String,List<String>>();
		String palias = exportFormatConfig.getDataSql().getPrimarySql().getAlias().toLowerCase();
		String[] pFileds = getpFileds(exportFormatConfig.getDataSql().getPrimarySql().getSql().toLowerCase());
		for(String pf:pFileds){
			getFileds(palias+"."+pf,exportFormatConfig.getDataSql().getFiledSqls(),
					afFiledSqls,pafAfs,null);
		}
		
		exportFormatConfig.setAfIndexFiledSqls(afIndexFiledSqls);
		exportFormatConfig.setAfFiledSqls(afFiledSqls);
		exportFormatConfig.setPafAfs(pafAfs);
		
	}
	
	/**
	 * 	 * 根据查询字段，找到所有的子查询   递归  Map<String,List<FiledSql>> a.f--filedsql,Map<String,List<String>> pa.f--a.f
	 * @param af   alias+"."+filed
	 * @param filedSqls
	 * @param c
	 * @param d
	 * @param paf 父 alias+"."+filed
	 */
	private void getFileds(String af,List<FiledSql> filedSqls,
			Map<String,List<FiledSql>> c,Map<String,List<String>> d,String paf){
		List<FiledSql> newFiled = new ArrayList<FiledSql>();
		newFiled.addAll(filedSqls);
		for(FiledSql fsql: filedSqls){
			if(fsql.getSql()!=null&&fsql.getSql().toLowerCase().contains(af)){
				//设置结果
				if(c.containsKey(af)){
					c.get(af).add(fsql);
				}else{
					List<FiledSql> fs = new ArrayList<FiledSql>();
					fs.add(fsql);
					c.put(af, fs);
				}
				if(paf!=null&&paf.length()>0){
					if(d.containsKey(paf)){
						d.get(paf).add(af);
					}else{
						List<String> fs = new ArrayList<String>();
						fs.add(af);
						d.put(paf, fs);
					}
				}
				newFiled.remove(fsql);
				String alias = fsql.getAlias().toLowerCase().trim();
				String[] sFiled = getpFileds(fsql.getSql().toLowerCase().trim());
				for(String sf:sFiled){
					getFileds(alias+"."+sf,newFiled,c,d,af);
				}
				
			}
		}
		
		
	}
	

	/**
	 * 查询字段
	 * @param sql
	 * @return
	 */
	private String[] getpFileds(String sql) {
		int start = sql.indexOf("select ")+"select ".length();
		int end = sql.indexOf(" from") ;
		String filed = sql.substring(start,end).trim();
		String[] fileds = null;
		if(filed.contains(".")){
			String[] temp = filed.split(",");
			fileds = new String[temp.length];
			for(int i=0;i<fileds.length;i++){
				fileds[i] = temp[i].substring(temp[i].indexOf(".")+1);
			}
		}else{
			fileds = filed.split(",");
		}
		return fileds;
	}

	private String[] getTableName(PrimarySql primarySql) {
		String sql = primarySql.getSql().toLowerCase();
		int start = sql.indexOf("from ")+"from ".length();
		int end = sql.length();
		if(sql.contains(" where ")){
			end = sql.indexOf(" where ");
		}
		String tableName = sql.substring(start,end).trim();
		if(tableName.contains(" "))
			tableName = tableName.substring(0,tableName.indexOf(" ")).trim();
		String[] tables = new String[]{tableName};
		this.exportFormatConfig.setTables(tableName);
		return tables;
	}
	

}
