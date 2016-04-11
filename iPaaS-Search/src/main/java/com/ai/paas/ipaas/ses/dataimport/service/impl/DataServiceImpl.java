package com.ai.paas.ipaas.ses.dataimport.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ai.paas.ipaas.ServiceUtil;
import com.ai.paas.ipaas.ses.dao.interfaces.SesDataimportDsMapper;
import com.ai.paas.ipaas.ses.dao.interfaces.SesDataimportSqlMapper;
import com.ai.paas.ipaas.ses.dao.interfaces.SesDataimportUserMapper;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesDataimportDs;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesDataimportDsCriteria;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesDataimportSql;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesDataimportSqlCriteria;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesDataimportUser;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesDataimportUserCriteria;
import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.impt.ImportData;
import com.ai.paas.ipaas.ses.dataimport.impt.OneDbImport;
import com.ai.paas.ipaas.ses.dataimport.impt.model.Result;
import com.ai.paas.ipaas.ses.dataimport.model.DataBaseAttr;
import com.ai.paas.ipaas.ses.dataimport.model.DataSql;
import com.ai.paas.ipaas.ses.dataimport.model.FiledSql;
import com.ai.paas.ipaas.ses.dataimport.model.PrimarySql;
import com.ai.paas.ipaas.ses.dataimport.service.IDataService;
import com.ai.paas.ipaas.ses.dataimport.util.ConfUtil;
import com.ai.paas.ipaas.ses.dataimport.util.DateUtil;
import com.ai.paas.ipaas.ses.dataimport.util.GsonUtil;
import com.ai.paas.ipaas.ses.dataimport.util.ImportUtil;
import com.ai.paas.ipaas.ses.dataimport.util.JdbcUtil;
import com.ai.paas.ipaas.ses.dataimport.util.ParamUtil;
import com.ai.paas.ipaas.ses.dataimport.util.SesUtil;
@Service
@Transactional(rollbackFor = Exception.class)
public class DataServiceImpl implements IDataService {
	private static final transient Logger log = LoggerFactory.getLogger(DataServiceImpl.class);


	@Override
	public String validateDataSource(List<DataBaseAttr> dataBaseAttrs ,Map<String,String>  userInfo) {
		Map<String,String> res = new HashMap<String,String>();

		if(dataBaseAttrs==null || dataBaseAttrs.isEmpty()){
			return "{\"CODE\":\"999\",\"MSG\":\"datasource is null.\"}";
		}
		if(dataBaseAttrs.size()==1){
			try {
				boolean resVa = validateDB(dataBaseAttrs.get(0));
				if(resVa){
					res.put("CODE", "000");
					res.put("MSG", "connect datasource success.");
				}else{
					res.put("CODE", "999");
					res.put("MSG", "can not connect datasource.");
				}
			} catch (Exception e) {
				log.error("--validateDB exception--:",e);
				res.put("CODE", "999");
				res.put("MSG", e.getMessage());
			}
			
			
		}
		return GsonUtil.objToGson(res);
	}
	

	/**
	 * 保存数据源
	 * @param dataBaseAttrs
	 */
	private void saveDs(Map<String,String> user,List<DataBaseAttr> dataBaseAttrs) throws Exception{
		if(user==null){
			return;
		}

		SesDataimportUserMapper userMapper = ServiceUtil.getMapper(SesDataimportUserMapper.class);
		SesDataimportUser suser = new SesDataimportUser();
		suser.setUserCode(user.get("userName"));
		suser.setUserId(user.get("userId"));
		suser.setSesSid(user.get("sid"));
		suser.setStatus(SesDataImportConstants.STATUS);
		SesDataimportUserCriteria c = new SesDataimportUserCriteria();
		c.createCriteria().andUserIdEqualTo(user.get("userId"))
		.andSesSidEqualTo((user.get("sid"))).andStatusEqualTo(SesDataImportConstants.STATUS);
		
		List<SesDataimportUser> users = userMapper.selectByExample(c);
		int id = 0;
		if(users!=null&&!users.isEmpty()){
			id = users.get(0).getId();
		}else{
			userMapper.insert(suser);
			users = userMapper.selectByExample(c);
			id = users.get(0).getId();
		}
		
		SesDataimportDsMapper dsMapper = ServiceUtil.getMapper(SesDataimportDsMapper.class);
		if(SesDataImportConstants.GROUP_ID_1==dataBaseAttrs.get(0).getGroupId()){
			SesDataimportDsCriteria dsc = new SesDataimportDsCriteria();
			dsc.createCriteria().andDuIdEqualTo(id)
			.andStatusEqualTo(SesDataImportConstants.STATUS)
			.andGroupIdEqualTo(dataBaseAttrs.get(0).getGroupId());//.andServiceIdEqualTo(suser.getSesSid())
			dsMapper.deleteByExample(dsc);
		}
		
		for(DataBaseAttr db :dataBaseAttrs){
			if(db.isOverwrite()){
				SesDataimportDsCriteria dsc = new SesDataimportDsCriteria();
				dsc.createCriteria().andDuIdEqualTo(id)
				.andStatusEqualTo(SesDataImportConstants.STATUS)
				.andGroupIdEqualTo(db.getGroupId())
				.andAliasEqualTo(db.getAlias());
				dsMapper.deleteByExample(dsc);
			}
			SesDataimportDs sesDs = new SesDataimportDs();
			sesDs.setDuId(id);
			sesDs.setStatus(SesDataImportConstants.STATUS);
			sesDs.setType(db.getType());
			sesDs.setGroupId(db.getGroupId());
			sesDs.setAlias(db.getAlias());

			Map<String,Object> info = new HashMap<String,Object>();
			if(SesDataImportConstants.COMMON_DB_TYPE==sesDs.getType()){
				info.put("database", db.getDatabase());
				info.put("ip", db.getIp());
				info.put("port", db.getPort());
				info.put("sid", db.getSid());
				info.put("username", db.getUsername());
				info.put("pwd", db.getPwd());
			}
			if(SesDataImportConstants.DBS_DB_TYPE==sesDs.getType()){
				info.put("user", db.getUser());
				info.put("serviceId", db.getServiceId());
				info.put("servicePwd", db.getServicePwd());
//				info.put("haveTXS", db.isHaveTXS());
				info.put("vsql", db.getVsql());
			}
			String infos = GsonUtil.objToGson(info);
			sesDs.setInfo(infos);
			dsMapper.insert(sesDs);
			
		}
		
		
	}

	@Override
	public String importData(HttpServletRequest reqeust) {
		Map<String,String> res = new HashMap<String,String>();
		Result result = null;
		long times = 0l;
//		initEsClient(userInfo);
		try {
			Map<String,String> userInfo = ParamUtil.getUser(reqeust);
			String userId =userInfo.get("userId");
			String userName = null;
			String password = null;
			String serviceId = userInfo.get("sid");
			String authAddress =ConfUtil.getProperty("AUTH_ADDR_URL");
			int groupId = Integer.valueOf(reqeust.getParameter("groupId"));
			
			List<DataBaseAttr>  dbAttr = this.loadDataSource(userId, serviceId,groupId);
			if(dbAttr==null||dbAttr.size()==0)
				throw new Exception("datasource is null.");
			DataSql dataSql = this.loadDataSql(userId, serviceId,groupId);
			if(dataSql==null||dataSql.getPrimarySql()==null)
				throw new Exception("sql is null.");
			long begin = System.currentTimeMillis();
			String sesUserInfo = userInfo.get("userName")+","+userInfo.get("sid")+","+userInfo.get("pwd").toString();

			if(SesDataImportConstants.GROUP_ID_1==groupId){
				OneDbImport oneImport = new OneDbImport(sesUserInfo,dbAttr.get(0),dataSql);
				result = oneImport.start();
			}else{
				String dsAlias = dataSql.getPrimarySql().getDrAlias();
				int type = 0;
				for(DataBaseAttr db : dbAttr){
					if(db.getType()==SesDataImportConstants.DBS_DB_TYPE){
						if(db.getAuthAddr()==null||db.getAuthAddr().length()==0){
							db.setAuthAddr(ConfUtil.getProperty("AUTH_ADDR_URL"));
						}
					}
					if(db.getAlias().equals(dsAlias)){
						userName = db.getUser();
						password = db.getServicePwd();
						serviceId = db.getServiceId();
						//主表是查询普通数据库
						type = db.getType();
					}
				}
				
				ImportData importData =new ImportData(userName,password,serviceId,authAddress,
						dataSql,dbAttr,sesUserInfo,type);
				result = importData.start();
			}
			times = System.currentTimeMillis()-begin;
			log.info("-------all------"+times);
			res.put("CODE", "000");
			if(result.getTotalNum()==result.getSucTotal()){
				res.put("MSG", " database totals:"+result.getTotalNum()+",ses totals:"+result.getSucTotal()+
						",used time:"+times+"ms.");
			}else if(result.getExcLog()!=null && result.getExcLog().length()>0){
				res.put("MSG", " database totals:"+result.getTotalNum()+",ses totals:"+result.getSucTotal()+
						",used time:"+times+"ms. error info:"+result.getExcLog());
			}
				
		} catch (Exception e) {
			res.put("CODE", "999");
			if(result!=null){
				res.put("MSG", " database totals:"+result.getTotalNum()+",ses totals:"+result.getSucTotal()+
					",used time:"+times+"ms. error info:"+e.getMessage());
			}else{
				res.put("MSG", e.getMessage());
			}
			log.error("--importData exception--",e);
		}
		
		return GsonUtil.objToGson(res);
	}
	


	private void initEsClient(Map<String, String> userInfo) {
		try {
			String userName = userInfo.get("userName").toString();
			String sid = userInfo.get("sid").toString();
			String pwd = userInfo.get("pwd").toString();
			String authAddr = ConfUtil.getProperty("AUTH_ADDR_URL");
			SesUtil.initSesClient(userName, sid, pwd, authAddr);
		} catch (Exception e) {
			log.warn("SesUtil.initSesClient",e);
		}
	}


	private boolean validateDB(DataBaseAttr attr) throws Exception{
		if(SesDataImportConstants.COMMON_DB_TYPE==attr.getType())
			return JdbcUtil.isAlived(attr);
		if(SesDataImportConstants.DBS_DB_TYPE==attr.getType())
			return JdbcUtil.dbsIsAlived(attr);
		return false;
	}


	@Override
	public List<DataBaseAttr> loadDataSource(String userId,String serviceId,int groupId) {
		List<DataBaseAttr> res = new ArrayList<DataBaseAttr>();
		try {
			SesDataimportUserMapper userMapper = ServiceUtil.getMapper(SesDataimportUserMapper.class);
			SesDataimportUserCriteria c = new SesDataimportUserCriteria();
			c.createCriteria().andUserIdEqualTo(userId).andSesSidEqualTo(serviceId);
			 List<SesDataimportUser> users = userMapper.selectByExample(c);
			 if(users==null || users.isEmpty())
				 return res;
			int id = userMapper.selectByExample(c).get(0).getId();
			
			SesDataimportDsMapper dsMapper = ServiceUtil.getMapper(SesDataimportDsMapper.class);
			SesDataimportDsCriteria sc = new SesDataimportDsCriteria();
			sc.createCriteria().andDuIdEqualTo(id).andGroupIdEqualTo(groupId);
			List<SesDataimportDs> datas = dsMapper.selectByExample(sc);
			if(datas!=null&&datas.size()>0)
				res = getDBfromSesDataimportDs(datas,groupId);
		} catch (Exception e) {
			log.error("--loadDataSource:"+e.getMessage(),e);
		}
		return res;
	}


	private int getIntValue(String str) {
		if(str==null||str.length()==0)
			return 0;
		if(str.contains(".")){
			return Integer.valueOf(str.substring(0,str.indexOf(".")));
		}else{
			return Integer.valueOf(str);
		}
	}


	@Override
	public DataSql loadDataSql(String userId,String serviceId,int groupId) {
		DataSql sql = new DataSql();
		SesDataimportUserMapper userMapper = ServiceUtil.getMapper(SesDataimportUserMapper.class);
		SesDataimportUserCriteria c = new SesDataimportUserCriteria();
		c.createCriteria().andUserIdEqualTo(userId).andSesSidEqualTo(serviceId);
		List<SesDataimportUser> users = userMapper.selectByExample(c);
		if(users==null||users.isEmpty())
			return sql;
		
		int id = userMapper.selectByExample(c).get(0).getId();
		
		SesDataimportSqlMapper sqlMapper = ServiceUtil.getMapper(SesDataimportSqlMapper.class);
		SesDataimportSqlCriteria sc = new SesDataimportSqlCriteria();
		sc.createCriteria().andDuIdEqualTo(id).andGroupIdEqualTo(groupId);
		List<SesDataimportSql> datas = sqlMapper.selectByExample(sc);
		if(datas!=null&&datas.size()>0){
			List<FiledSql> filedSqls = new ArrayList<FiledSql>();
			PrimarySql p = new PrimarySql();
			if(groupId==SesDataImportConstants.GROUP_ID_1){
				String info = datas.get(0).getInfo();
				sql.setId(datas.get(0).getId());
				Map infos = GsonUtil.gsonToObject(info, HashMap.class);
				if(datas.get(0).getIsPrimary()==SesDataImportConstants.PRIMARY){
					p.setAlias(datas.get(0).getAlias());
					p.setDrAlias(datas.get(0).getDsAlias());
					p.setSql(infos.get("sql").toString());
					sql.setPrimarySql(p);
				}
			}else{
				for(SesDataimportSql sq :datas){
					String info = sq.getInfo();
					Map infos = GsonUtil.gsonToObject(info, HashMap.class);
					if(sq.getIsPrimary()==SesDataImportConstants.PRIMARY){
						p.setAlias(sq.getAlias());
						p.setDrAlias(sq.getDsAlias());
						p.setPrimaryKey(infos.get("primaryKey").toString());
						p.setSql(infos.get("sql").toString());
						sql.setPrimarySql(p);
					}else{
						FiledSql fSql = new FiledSql();
						fSql.setAlias(sq.getAlias());
						fSql.setDrAlias(sq.getDsAlias());
						fSql.setRelation(Integer.valueOf(infos.get("relation").toString()));
						fSql.setMapObj(infos.get("mapObj")==null?false:
							Boolean.valueOf(infos.get("mapObj").toString()));
						if(infos.containsKey("indexAlias"))
							fSql.setIndexAlias(infos.get("indexAlias").toString());
						if(infos.containsKey("indexSql"))
							fSql.setIndexSql(infos.get("indexSql").toString());
						fSql.setSql(infos.get("filedSql").toString());
						filedSqls.add(fSql);
					}
					sql.setFiledSqls(filedSqls);
				}
			}

		}
		return sql;
	}


	@Override
	public String saveSql(HttpServletRequest reqeust, Map<String,String>  userInfo) {
		if(userInfo==null){
			return "{\"CODE\":\"999\",\"MSG\":\"datasource is null.\"}";
		}
		Map<String,String> res = new HashMap<String,String>();

		try {
			String groupId = reqeust.getParameter("groupId");
			int uId = getUserId(reqeust,userInfo);
			SesDataimportSqlMapper sqlMapper = ServiceUtil.getMapper(SesDataimportSqlMapper.class);
			SesDataimportDsMapper dsMapper = ServiceUtil.getMapper(SesDataimportDsMapper.class);
			
			SesDataimportSqlCriteria ssc = new SesDataimportSqlCriteria();
			
			if(Integer.valueOf(groupId)==SesDataImportConstants.GROUP_ID_1){
				ssc.createCriteria().andDuIdEqualTo(uId)
				.andStatusEqualTo(SesDataImportConstants.STATUS)
				.andGroupIdEqualTo(Integer.valueOf(groupId));
				sqlMapper.deleteByExample(ssc);
			}
			if(Integer.valueOf(groupId)==SesDataImportConstants.GROUP_ID_2){
				String isPriStr = reqeust.getParameter("isPrimary");
				if(isPriStr!=null&&isPriStr.length()>0&&Boolean.valueOf(isPriStr)){
					ssc.createCriteria().andDuIdEqualTo(uId)
					.andStatusEqualTo(SesDataImportConstants.STATUS)
					.andGroupIdEqualTo(Integer.valueOf(groupId))
					.andIsPrimaryEqualTo(SesDataImportConstants.PRIMARY);
					sqlMapper.deleteByExample(ssc);
				}
				
				String overwriteStr = reqeust.getParameter("overwrite");
				if(overwriteStr!=null&&overwriteStr.length()>0&&Boolean.valueOf(overwriteStr)){
					if(isPriStr!=null&&isPriStr.length()>0){
						if(Boolean.valueOf(isPriStr)){
							ssc.createCriteria().andDuIdEqualTo(uId)
							.andStatusEqualTo(SesDataImportConstants.STATUS)
							.andGroupIdEqualTo(Integer.valueOf(groupId))
							.andIsPrimaryEqualTo(SesDataImportConstants.PRIMARY);
						}else{
							ssc.createCriteria().andDuIdEqualTo(uId)
							.andStatusEqualTo(SesDataImportConstants.STATUS)
							.andGroupIdEqualTo(Integer.valueOf(groupId))
							.andAliasEqualTo(reqeust.getParameter("falias"))
							.andIsPrimaryEqualTo(SesDataImportConstants.IN_PRIMARY);
						}
						
					}
					sqlMapper.deleteByExample(ssc);
				}
			}

			SesDataimportSql sql = new SesDataimportSql();
			if(Integer.valueOf(groupId)==SesDataImportConstants.GROUP_ID_1){
				DataBaseAttr dbAttr = ParamUtil.getDs(reqeust,null).get(0);
				sql.setAlias(dbAttr.getAlias());
				sql.setDsAlias(dbAttr.getAlias());
				sql.setIsPrimary(SesDataImportConstants.PRIMARY);
				sql.setStatus(SesDataImportConstants.STATUS);
				sql.setGroupId(dbAttr.getGroupId());
				Map<String,String> info = new HashMap<String,String>();
				info.put("sql", reqeust.getParameter("sql"));
				sql.setInfo(GsonUtil.objToGson(info));
				
				if(dbAttr.getId()<1){
					SesDataimportDsCriteria sc = new SesDataimportDsCriteria();
	
					sc.createCriteria().andDuIdEqualTo(uId)
					.andGroupIdEqualTo(dbAttr.getGroupId());
					List<SesDataimportDs> dss = dsMapper.selectByExample(sc);
					SesDataimportDs ds = dss.get(0);
					sql.setDsId(ds.getId());
				}else{
					sql.setDsId(dbAttr.getId());
				}
				
			}else{
				String isPriStr = reqeust.getParameter("isPrimary");
				if(isPriStr!=null&&isPriStr.length()>0&&Boolean.valueOf(isPriStr)){
					sql.setAlias(reqeust.getParameter("alias"));
					sql.setDsAlias(reqeust.getParameter("drAlias"));
					sql.setIsPrimary(SesDataImportConstants.PRIMARY);
					sql.setStatus(SesDataImportConstants.STATUS);
					sql.setGroupId(Integer.valueOf(groupId));

					Map<String,String> info = new HashMap<String,String>();
					info.put("primaryKey", reqeust.getParameter("primaryKey"));
					info.put("sql", reqeust.getParameter("sql"));
					sql.setInfo(GsonUtil.objToGson(info));
					
					SesDataimportDsCriteria sc = new SesDataimportDsCriteria();

					sc.createCriteria().andDuIdEqualTo(uId)
					.andAliasEqualTo(reqeust.getParameter("drAlias"))
					.andGroupIdEqualTo(SesDataImportConstants.GROUP_ID_2);
					List<SesDataimportDs> dss = dsMapper.selectByExample(sc);
					SesDataimportDs ds = null;
					ds = dss.get(0);
					sql.setDsId(ds.getId());
				}else{
					String fdrAlias = reqeust.getParameter("fdrAlias");
					sql.setAlias(reqeust.getParameter("falias"));
					sql.setDsAlias(reqeust.getParameter("fdrAlias"));
					sql.setIsPrimary(SesDataImportConstants.IN_PRIMARY);
					sql.setStatus(SesDataImportConstants.STATUS);
					sql.setGroupId(Integer.valueOf(groupId));
					
					Map<String,String> info = new HashMap<String,String>();
					info.put("relation", reqeust.getParameter("relation"));
					info.put("mapObj", reqeust.getParameter("mapObj"));
					if(fdrAlias!=null&&fdrAlias.split("_").length==2){
						info.put("indexAlias", reqeust.getParameter("indexAlias"));
						info.put("indexSql", reqeust.getParameter("indexSql"));
					}
					info.put("filedSql", reqeust.getParameter("fsql"));
					sql.setInfo(GsonUtil.objToGson(info));
					
					SesDataimportDsCriteria sc = new SesDataimportDsCriteria();

					sc.createCriteria().andDuIdEqualTo(uId)
					.andAliasEqualTo(fdrAlias)
					.andGroupIdEqualTo(SesDataImportConstants.GROUP_ID_2);
					List<SesDataimportDs> dss = dsMapper.selectByExample(sc);
					SesDataimportDs ds = null;
					ds = dss.get(0);
					sql.setDsId(ds.getId());
				}
			}
			sql.setDuId(uId);
			sqlMapper.insert(sql);
			res.put("CODE", "000");
			res.put("MSG", "save sql success.");

		} catch (Exception e) {
			res.put("CODE", "999");
			res.put("MSG", e.getMessage());
			log.error("--save sql exception--:",e);
		}
		return GsonUtil.objToGson(res);
	}
	
	private int getUserId(HttpServletRequest request,Map<String, String> userInfo) throws Exception {
		int uId = 0;
		String uIdParam = request.getParameter("uId");
		if(uIdParam!=null&&uIdParam.length()>0){
			uId =  Integer.valueOf(uIdParam);
		}
		if(uId==0){
			SesDataimportUserMapper userMapper = ServiceUtil.getMapper(SesDataimportUserMapper.class);
			SesDataimportUserCriteria c = new SesDataimportUserCriteria();
			c.createCriteria().andUserIdEqualTo(userInfo.get("userId"))
			.andSesSidEqualTo((userInfo.get("sid"))).andStatusEqualTo(SesDataImportConstants.STATUS);
			List<SesDataimportUser> users = userMapper.selectByExample(c);
			SesDataimportUser dataUser = null;
			if(users!=null && !users.isEmpty()){
				dataUser = users.get(0);
				uId = dataUser.getId();
			}else{
				throw new Exception("user is null.");
			}
		}
		return uId;
	}


	/**保存数据源*/
	@Override
	public String saveDs(List<DataBaseAttr> dataBaseAttrs,
			Map<String, String> userInfo) {
		Map<String,String> res = new HashMap<String,String>();

		if(dataBaseAttrs==null || dataBaseAttrs.isEmpty()){
			return "{\"CODE\":\"999\",\"MSG\":\"DataBase is null.\"}";
		}
		try {
			saveDs(userInfo,dataBaseAttrs);
			res.put("CODE", "000");
			res.put("MSG", "save datasource success.");
		} catch (Exception e) {
			res.put("CODE", "999");
			res.put("MSG", e.getMessage());
			log.error("--save dataresource exception--:",e);
		}
		return GsonUtil.objToGson(res);
	}

	/**验证Sql*/
	@Override
	public String validateSql(List<DataBaseAttr> dataSources,Map<String,String> userInfo,HttpServletRequest request) {
		Map<String,String> res = new HashMap<String,String>();

		String groupId = request.getParameter("groupId");
		if(groupId!=null&&groupId.length()>0){
			try {
				boolean resVa = false;
				String sql = "";
				DataBaseAttr db = null;
				if(Integer.valueOf(groupId)==SesDataImportConstants.GROUP_ID_1){
					sql = request.getParameter("sql");
					dataSources = ParamUtil.getDs(request,null);
					if(dataSources==null || dataSources.isEmpty())
						return "{\"CODE\":\"999\",\"MSG\":\"datasrouce is null.\"}";
					db = dataSources.get(0);
				}
				if(Integer.valueOf(groupId)==SesDataImportConstants.GROUP_ID_2){
					String isPriStr = request.getParameter("isPrimary");
					if(isPriStr!=null&&isPriStr.length()>0&&Boolean.valueOf(isPriStr)){
						sql = request.getParameter("sql");
						int uId = getUserId(request,userInfo);
						SesDataimportDsCriteria sc = new SesDataimportDsCriteria();

						sc.createCriteria().andDuIdEqualTo(uId)
						.andAliasEqualTo(request.getParameter("drAlias"))
						.andGroupIdEqualTo(SesDataImportConstants.GROUP_ID_2);
						SesDataimportDsMapper dsMapper = ServiceUtil.getMapper(SesDataimportDsMapper.class);
						List<SesDataimportDs> dss = dsMapper.selectByExample(sc);
						db = getDBfromSesDataimportDs(dss,SesDataImportConstants.GROUP_ID_2).get(0);
					}else{
						//TODO
					}
				}
				resVa = JdbcUtil.validateSql(db,sql);

				if(resVa){
					res.put("CODE", "000");
					res.put("MSG", "config sql success.");
				}else{
					res.put("CODE", "999");
					res.put("MSG", "sql is invalite.");
				}
			} catch (Exception e) {
				log.error("--validateDB exception--:",e);
				res.put("CODE", "999");
				res.put("MSG", e.getMessage());
			}
		}
		return GsonUtil.objToGson(res);
	}


	private List<DataBaseAttr> getDBfromSesDataimportDs(List<SesDataimportDs> dss,int groupId) throws Exception {
		if(dss==null || dss.isEmpty())
			throw new Exception("datasource is null.");
		
		List<DataBaseAttr> res = new ArrayList<DataBaseAttr>();
		for(SesDataimportDs ds :dss){
			DataBaseAttr attr = new DataBaseAttr();
			attr.setId(ds.getId());
			attr.setAlias(ds.getAlias());
			attr.setType(ds.getType());
			String info = ds.getInfo();
			attr.setGroupId(groupId);
			attr.setuId(ds.getDuId());
			Map infos = GsonUtil.gsonToObject(info, HashMap.class);
			if(SesDataImportConstants.COMMON_DB_TYPE==ds.getType()){
				attr.setDatabase(getIntValue(infos.get("database").toString()));
				attr.setIp(infos.get("ip").toString());
				attr.setPort(getIntValue(infos.get("port").toString()));
				attr.setSid(infos.get("sid").toString());
				attr.setUsername(infos.get("username").toString());
				attr.setPwd(infos.get("pwd").toString());
			}
			if(SesDataImportConstants.DBS_DB_TYPE==ds.getType()){
				attr.setUser(infos.get("user").toString());
				attr.setServiceId(infos.get("serviceId").toString());
				attr.setServicePwd(infos.get("servicePwd").toString());
				attr.setVsql(infos.get("vsql").toString());
			}
			
			res.add(attr);
		}
		return res;
	}


	@Override
	public String deleteDs(List<DataBaseAttr> dataSources,
			Map<String, String> userInfo) {
		Map<String,String> res = new HashMap<String,String>();

		if(dataSources==null || dataSources.isEmpty()){
			return "{\"CODE\":\"999\",\"MSG\":\"datasource is null.\"}";
		}
		try {
			//du_id
			int id = 0;
			if(dataSources.get(0).getuId()<1){
				List<SesDataimportUser> sesUser = getSesUser(userInfo);
				if(sesUser!=null&&!sesUser.isEmpty()){
					id = sesUser.get(0).getId();
				}else{
					throw new Exception("ses user is null.");
				}
			}else{
				id = dataSources.get(0).getuId(); 
			}
			
			SesDataimportDsMapper dsMapper = ServiceUtil.getMapper(SesDataimportDsMapper.class);

			SesDataimportDsCriteria dsc = new SesDataimportDsCriteria();
			if(dataSources.get(0).getGroupId()==SesDataImportConstants.GROUP_ID_1){
				dsc.createCriteria().andDuIdEqualTo(id)
				.andStatusEqualTo(SesDataImportConstants.STATUS)
				.andGroupIdEqualTo(dataSources.get(0).getGroupId());
			}else if(dataSources.get(0).getGroupId()==SesDataImportConstants.GROUP_ID_2){
				dsc.createCriteria().andDuIdEqualTo(id)
				.andStatusEqualTo(SesDataImportConstants.STATUS)
				.andGroupIdEqualTo(dataSources.get(0).getGroupId())
				.andAliasEqualTo(dataSources.get(0).getAlias());
			}
			dsMapper.deleteByExample(dsc);

			res.put("CODE", "000");
			res.put("MSG", "delete datasource success.");
		} catch (Exception e) {
			res.put("CODE", "999");
			res.put("MSG", e.getMessage());
			log.error("--delete dataresource exception--:",e);
		}
		return GsonUtil.objToGson(res);
	}


	private List<SesDataimportUser> getSesUser(Map<String, String> userInfo) throws Exception {
		SesDataimportUserMapper userMapper = ServiceUtil.getMapper(SesDataimportUserMapper.class);
		SesDataimportUser suser = new SesDataimportUser();
		suser.setUserCode(userInfo.get("userName"));
		suser.setUserId(userInfo.get("userId"));
		suser.setSesSid(userInfo.get("sid"));
		suser.setStatus(SesDataImportConstants.STATUS);
		SesDataimportUserCriteria c = new SesDataimportUserCriteria();
		c.createCriteria().andUserIdEqualTo(userInfo.get("userId"))
		.andSesSidEqualTo((userInfo.get("sid"))).andStatusEqualTo(SesDataImportConstants.STATUS);
		
		List<SesDataimportUser> users = userMapper.selectByExample(c);
		return users;
	}


	@Override
	public String deleteSql(HttpServletRequest reqeust,
			Map<String, String> userInfo) {
		Map<String,String> res = new HashMap<String,String>();

		try {
			String groupId = reqeust.getParameter("groupId");

			if(groupId!=null&&groupId.length()>0){
				if(Integer.valueOf(groupId)==SesDataImportConstants.GROUP_ID_1){
					String sqlId = reqeust.getParameter("sql.id");
					if(sqlId==null||"".equals(sqlId))
						throw new Exception(" sql is not exist.");
					SesDataimportSqlMapper sqlMapper = ServiceUtil.getMapper(SesDataimportSqlMapper.class);

					SesDataimportSqlCriteria dsc = new SesDataimportSqlCriteria();
					dsc.createCriteria().andIdEqualTo(Integer.valueOf(sqlId));
					sqlMapper.deleteByExample(dsc);
				}
				if(Integer.valueOf(groupId)==SesDataImportConstants.GROUP_ID_2){
					String isPriStr = reqeust.getParameter("isPrimary");

					if(isPriStr!=null&&isPriStr.length()>0){
						int uId = getUserId(reqeust,userInfo);
						SesDataimportSqlMapper sqlMapper = ServiceUtil.getMapper(SesDataimportSqlMapper.class);
						SesDataimportSqlCriteria ssc = new SesDataimportSqlCriteria();
						if(Boolean.valueOf(isPriStr)){
							ssc.createCriteria().andDuIdEqualTo(uId)
							.andGroupIdEqualTo(SesDataImportConstants.GROUP_ID_2)
							.andIsPrimaryEqualTo(SesDataImportConstants.PRIMARY);
						}else{
							ssc.createCriteria().andDuIdEqualTo(uId)
							.andGroupIdEqualTo(SesDataImportConstants.GROUP_ID_2)
							.andIsPrimaryEqualTo(SesDataImportConstants.IN_PRIMARY)
							.andAliasEqualTo(reqeust.getParameter("falias"));
						}
						sqlMapper.deleteByExample(ssc);

						
					}
						
				}
			}
			
			
			res.put("CODE", "000");
			res.put("MSG", "delete sql success");
		} catch (Exception e) {
			log.error("--validateDB exception--:",e);
			res.put("CODE", "999");
			res.put("MSG", e.getMessage());
		}
		return GsonUtil.objToGson(res);
		
	}


	@Override
	public String running(HttpServletRequest reqeust) {
		Map<String,String> res = new HashMap<String,String>();

		Map<String,String> userInfo = ParamUtil.getUser(reqeust);
		String sesUserInfo = userInfo.get("userName")+SesDataImportConstants.SPLIT_STR+userInfo.get("sid");		
		Result result = ImportUtil.getRunning(sesUserInfo);
		res.put("CODE", "000");
		String er = "";
		if(result==null){
			er = " not begin import to SES"; 
		}else{
			er = result.getLastExcLog();
			if(er!=null&&er.length()>0){
				//stop load
				res.put("CODE", "999");
			}
		}
		if(er!=null&&er.length()>0){
			if(result==null){
				res.put("MSG", "<p><span>"+DateUtil.getTimes()+"</span> "+er+"...</p>");
			}else{
				res.put("MSG", "<p><span>"+DateUtil.getTimes()+"</span> success num:"+result.getSucTotal()+","+er+"...</p>");
			}
		}else{
			res.put("MSG", "<p><span>"+DateUtil.getTimes()+"</span> success num:"+result.getSucTotal()+"...</p>");
		}
		return GsonUtil.objToGson(res);
	}
	
	

}
