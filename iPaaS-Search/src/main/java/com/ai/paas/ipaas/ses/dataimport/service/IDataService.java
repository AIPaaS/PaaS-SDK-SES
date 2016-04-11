package com.ai.paas.ipaas.ses.dataimport.service;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import com.ai.paas.ipaas.ses.dataimport.model.DataBaseAttr;
import com.ai.paas.ipaas.ses.dataimport.model.DataSql;

public interface IDataService {
	/**
	 * 验证数据源 合法性
	 * @param dataSources
	 * @return
	 */
	String validateDataSource(List<DataBaseAttr> dataSources,Map<String,String> userInfo) ;
	
	/**
	 * 保存数据源
	 * @param dataSources
	 * @return
	 */
	String saveDs(List<DataBaseAttr> dataSources,Map<String,String> userInfo) ;
	
	/**
	 * 删除数据源
	 * @param dataSources
	 * @return
	 */
	String deleteDs(List<DataBaseAttr> dataSources,Map<String,String> userInfo) ;
	
	
	
	/**
	 * 验证sql 合法性
	 * @param dataSources
	 * @return
	 */
	String validateSql(List<DataBaseAttr> dataSources,Map<String,String> userInfo,HttpServletRequest request) ;
	/**
	 * 保存sql
	 * @param reqeust
	 * @return
	 */
	String saveSql(HttpServletRequest reqeust,Map<String,String> userInfo) ;
	/**
	 * 删除sql
	 * @param reqeust
	 * @return
	 */
	String deleteSql(HttpServletRequest reqeust,Map<String,String> userInfo) ;
	
	/**
	 * 加载数据源
	 * @param userId
	 * @return
	 */
	List<DataBaseAttr> loadDataSource(String userId,String serviceId,int groupId) ;
	
	/**
	 * 加载已配置的sql
	 * @param userId
	 * @return
	 */
	DataSql loadDataSql(String userId,String serviceId,int groupId) ;
	
	/**
	 * 导入数据
	 * @param reqeust
	 * @return
	 */
	String importData(HttpServletRequest reqeust) ;
	/**
	 * 导入进度
	 * @param reqeust
	 * @return
	 */
	String running(HttpServletRequest reqeust) ;
	
	
}
