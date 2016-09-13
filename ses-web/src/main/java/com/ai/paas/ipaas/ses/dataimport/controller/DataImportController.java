package com.ai.paas.ipaas.ses.dataimport.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ai.paas.ipaas.PaasRuntimeException;
import com.ai.paas.ipaas.search.ISearchClient;
import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.impt.ImportData;
import com.ai.paas.ipaas.ses.dataimport.impt.OneDbImport;
import com.ai.paas.ipaas.ses.dataimport.impt.model.Result;
import com.ai.paas.ipaas.ses.dataimport.util.ConfUtil;
import com.ai.paas.ipaas.ses.dataimport.util.DateUtil;
import com.ai.paas.ipaas.ses.dataimport.util.GsonUtil;
import com.ai.paas.ipaas.ses.dataimport.util.ImportUtil;
import com.ai.paas.ipaas.ses.dataimport.util.JdbcUtil;
import com.ai.paas.ipaas.ses.dataimport.util.ParamUtil;
import com.ai.paas.ipaas.ses.dataimport.util.SesUtil;
import com.ai.paas.ipaas.ses.manage.rest.interfaces.IRPCDataSource;
import com.ai.paas.ipaas.vo.ses.RPCDataSource;
import com.ai.paas.ipaas.vo.ses.SesDataSourceInfo;
import com.ai.paas.ipaas.vo.ses.SesIndexSqlInfo;

@Controller
@RequestMapping(value = "/dataimport")
public class DataImportController {
	private static final transient Logger log = LoggerFactory
			.getLogger(DataImportController.class);

	@Autowired
	private IRPCDataSource dataService;

	@RequestMapping(value = "/toOne")
	public String toOne(HttpServletRequest request) {

		List<SesDataSourceInfo> dbAttrs = dataService.getIndexDataSources(
				ParamUtil.getUser(request).get(
						SesDataImportConstants.USER_ID_STR),
				ParamUtil.getUser(request).get(SesDataImportConstants.SID_STR),
				SesDataImportConstants.GROUP_ID_1);
		if (dbAttrs != null && dbAttrs.size() > 0) {
			request.setAttribute("oneds", dbAttrs.get(0));
		}
		SesIndexSqlInfo sql = dataService.getIndexDataSql(
				ParamUtil.getUser(request).get(
						SesDataImportConstants.USER_ID_STR),
				ParamUtil.getUser(request).get(SesDataImportConstants.SID_STR),
				SesDataImportConstants.GROUP_ID_1);
		request.setAttribute("onesql", sql);

		return "import/one";
	}

	@RequestMapping(value = "/toMany")
	public String toMany(HttpServletRequest request) {
		List<SesDataSourceInfo> dbAttrs = dataService.getIndexDataSources(
				ParamUtil.getUser(request).get(
						SesDataImportConstants.USER_ID_STR),
				ParamUtil.getUser(request).get(SesDataImportConstants.SID_STR),
				SesDataImportConstants.GROUP_ID_2);
		if (dbAttrs != null && dbAttrs.size() > 0) {
			request.setAttribute("manyds", dbAttrs);
		}
		SesIndexSqlInfo sql = dataService.getIndexDataSql(
				ParamUtil.getUser(request).get(
						SesDataImportConstants.USER_ID_STR),
				ParamUtil.getUser(request).get(SesDataImportConstants.SID_STR),
				SesDataImportConstants.GROUP_ID_2);
		request.setAttribute("manysql", sql);
		return "import/many";
	}

	/** 测试数据源 */
	@ResponseBody
	@RequestMapping(value = "/validateSource")
	public String validateSource(HttpServletRequest request) {
		try {
			return JdbcUtil.validateDataSource(ParamUtil.getDs(request, null),
					ParamUtil.getUser(request));
		} catch (Exception e) {
			log.error("", e);
			return ParamUtil.getERRORMSG(e.getMessage());

		}
	}

	/** 保存数据源 */
	@ResponseBody
	@RequestMapping(value = "/saveDs")
	public String saveDs(HttpServletRequest request) {
		try {
			Map<String, String> res = new HashMap<String, String>();
			List<SesDataSourceInfo> dataSources = ParamUtil
					.getDs(request, null);
			if (dataSources == null || dataSources.isEmpty()) {
				return "{\"CODE\":\"999\",\"MSG\":\"datasource is null.\"}";
			}
			RPCDataSource rpcDataSource = new RPCDataSource();
			rpcDataSource.setUserInfo(ParamUtil.getUser(request));
			rpcDataSource.setDataSources(ParamUtil.getDs(request, null));
			dataService.saveDataSource(rpcDataSource);
			res.put("CODE", "000");
			res.put("MSG", "save datasource success.");
			return GsonUtil.objToGson(res);
		} catch (Exception e) {
			log.error("", e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}

	/** 删除数据源 */
	@ResponseBody
	@RequestMapping(value = "/deleteDs")
	public String deleteDs(HttpServletRequest request) {
		try {
			Map<String, String> res = new HashMap<String, String>();
			List<SesDataSourceInfo> dataSources = ParamUtil.getDs(request,
					"delete");
			if (dataSources == null || dataSources.isEmpty()) {
				return "{\"CODE\":\"999\",\"MSG\":\"datasource is null.\"}";
			}
			RPCDataSource rpcDataSource = new RPCDataSource();
			rpcDataSource.setDataSources(dataSources);
			rpcDataSource.setUserInfo(ParamUtil.getUser(request));
			dataService.deleteDataSource(rpcDataSource);
			res.put("CODE", "000");
			res.put("MSG", "delete datasource success.");
			return GsonUtil.objToGson(res);
		} catch (Exception e) {
			log.error("", e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}

	/** 测试Sql */
	@ResponseBody
	@RequestMapping(value = "/validateSql")
	public String validateSql(HttpServletRequest request) {
		try {
			// 这里需做准备
			Map<String, String> userInfo = ParamUtil.getUser(request);
			int uId = getUserId(request, userInfo);
			String groupId = request.getParameter("groupId");
			List<SesDataSourceInfo> oraDataSources = null;
			if (("" + SesDataImportConstants.GROUP_ID_2).equals(groupId)) {
				oraDataSources = dataService.getDataSource(uId,
						request.getParameter("drAlias"),
						SesDataImportConstants.GROUP_ID_2);
			}
			return JdbcUtil.validateSql(null, oraDataSources,
					ParamUtil.getUser(request), request);
		} catch (Exception e) {
			log.error("", e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}

	@ResponseBody
	@RequestMapping(value = "/saveSql")
	public String saveSql(HttpServletRequest request) {
		try {
			Map<String, String> res = new HashMap<String, String>();
			Map<String, String> dbInfo = new HashMap<String, String>();
			dbInfo.put("groupId", request.getParameter("groupId"));
			dbInfo.put("uId",
					"" + getUserId(request, ParamUtil.getUser(request)));
			dbInfo.put("isPrimary", request.getParameter("isPrimary"));
			dbInfo.put("alias", request.getParameter("alias"));
			dbInfo.put("falias", request.getParameter("falias"));
			dbInfo.put("overwrite", request.getParameter("overwrite"));
			dbInfo.put("sql", request.getParameter("sql"));
			dbInfo.put("drAlias", request.getParameter("drAlias"));
			dbInfo.put("fdrAlias", request.getParameter("fdrAlias"));
			dbInfo.put("fsql", request.getParameter("fsql"));
			dbInfo.put("relation", request.getParameter("relation"));
			dbInfo.put("mapObj", request.getParameter("mapObj"));
			
			RPCDataSource rpcDataSource = new RPCDataSource();
			rpcDataSource.setDbInfo(dbInfo);
			// 多库时不用设置数据源了
			if (("" + SesDataImportConstants.GROUP_ID_2).equals(request
					.getParameter("groupId"))) {
				rpcDataSource.setDbAttr(null);
			} else {
				rpcDataSource.setDbAttr(ParamUtil.getDs(request, null).get(0));
			}
			rpcDataSource.setUserInfo(ParamUtil.getUser(request));
			dataService.saveIndexDataSql(rpcDataSource);

			res.put("CODE", "000");
			res.put("MSG", "save datasource success.");
			return GsonUtil.objToGson(res);
		} catch (Exception e) {
			log.error("", e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}

	@ResponseBody
	@RequestMapping(value = "/deleteSql")
	public String deleteSql(HttpServletRequest request) {
		try {
			Map<String, String> res = new HashMap<String, String>();
			Map<String, String> sqlInfo = new HashMap<String, String>();
			sqlInfo = ParamUtil.getSqlInfo(request, "delete");
			if (sqlInfo == null || sqlInfo.isEmpty()) {
				return "{\"CODE\":\"999\",\"MSG\":\"sql is null.\"}";
			}
			RPCDataSource rpcDataSource = new RPCDataSource();
			rpcDataSource.setSqlInfo(sqlInfo);
			rpcDataSource.setUserInfo(ParamUtil.getUser(request));
			dataService.deleteIndexDataSql(rpcDataSource);
			res.put("CODE", "000");
			res.put("MSG", "delete datasource success.");
			return GsonUtil.objToGson(res);
		} catch (Exception e) {
			log.error("", e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}

	@ResponseBody
	@RequestMapping(value = "/clear", produces = "application/text;charset=utf-8")
	public String clearData(HttpServletRequest request) {
		try {
			return startClearData(request);
		} catch (Exception e) {
			log.error("", e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}

	@ResponseBody
	@RequestMapping(value = "/import", produces = "application/text;charset=utf-8")
	public String importData(HttpServletRequest request) {
		try {
			return startImportData(request);
		} catch (Exception e) {
			log.error("", e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}

	@ResponseBody
	@RequestMapping(value = "/running")
	public String running(HttpServletRequest request) {
		try {
			return getRunningInfo(request);
		} catch (Exception e) {
			log.error("", e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}

	@ResponseBody
	@RequestMapping(value = "/loadDs")
	public String loadDs(HttpServletRequest request) {
		try {
			List<SesDataSourceInfo> dbAttrs = dataService.getIndexDataSources(
					ParamUtil.getUser(request).get(
							SesDataImportConstants.USER_ID_STR),
					ParamUtil.getUser(request).get(
							SesDataImportConstants.SID_STR),
					SesDataImportConstants.GROUP_ID_2);
			if (dbAttrs != null && dbAttrs.size() > 0) {
				request.setAttribute("manyds", dbAttrs);
			}
			return "{\"CODE\":\"000\",\"MSG\":\""
					+ ParamUtil.getDsInfo(dbAttrs) + "\"}";
		} catch (Exception e) {
			log.error("", e);
			return ParamUtil.getERRORMSG(e.getMessage());
		}
	}

	private int getUserId(HttpServletRequest request,
			Map<String, String> userInfo) throws Exception {
		int uId = 0;
		String uIdParam = request.getParameter("uId");
		if (uIdParam != null && uIdParam.length() > 0) {
			uId = Integer.valueOf(uIdParam);
		}
		if (uId == 0) {
			String tempId = dataService.getDataSourceUserPK(
					userInfo.get("userId"), userInfo.get("sid"));
			if (null == tempId) {
				throw new PaasRuntimeException(
						"Can not find the user data source pk!" + userInfo);
			}
			uId = Integer.parseInt(tempId);
		}
		return uId;
	}

	private String getRunningInfo(HttpServletRequest request) {
		Map<String, String> res = new HashMap<String, String>();

		Map<String, String> userInfo = ParamUtil.getUser(request);
		String sesUserInfo = userInfo.get("userName")
				+ SesDataImportConstants.SPLIT_STR + userInfo.get("sid");
		Result result = ImportUtil.getRunning(sesUserInfo);
		res.put("CODE", "000");
		String er = "";
		if (result == null) {
			er = " not begin import to SES";
		} else {
			er = result.getLastExcLog();
			if (er != null && er.length() > 0) {
				// stop load
				res.put("CODE", "999");
			}
		}
		if (er != null && er.length() > 0) {
			if (result == null) {
				res.put("MSG", "<p><span>" + DateUtil.getTimes() + "</span> "
						+ er + "...</p>");
			} else {
				res.put("MSG", "<p><span>" + DateUtil.getTimes()
						+ "</span> success num:" + result.getSucTotal() + ","
						+ er + "...</p>");
			}
		} else {
			res.put("MSG", "<p><span>" + DateUtil.getTimes()
					+ "</span> success num:" + result.getSucTotal() + "...</p>");
		}
		return GsonUtil.objToGson(res);
	}

	private String startImportData(HttpServletRequest request) {
		Map<String, String> res = new HashMap<String, String>();
		Result result = null;
		long times = 0l;
		try {
			Map<String, String> userInfo = ParamUtil.getUser(request);
			String userId = userInfo.get("userId");
			String userName = null;
			String password = null;
			String serviceId = userInfo.get("sid");
			String authAddress = ConfUtil.getProperty("AUTH_ADDR_URL")
					+ "/auth";
			int groupId = Integer.valueOf(request.getParameter("groupId"));

			List<SesDataSourceInfo> dbAttr = dataService.getIndexDataSources(
					userId, serviceId, groupId);
			if (dbAttr == null || dbAttr.size() == 0)
				throw new Exception("datasource is null.");
			SesIndexSqlInfo dataSql = dataService.getIndexDataSql(userId,
					serviceId, groupId);
			if (dataSql == null || dataSql.getPrimarySql() == null)
				throw new Exception("sql is null.");
			long begin = System.currentTimeMillis();
			String sesUserInfo = userInfo.get("pId") + ","
					+ userInfo.get("sid") + ","
					+ userInfo.get("pwd").toString();

			if (SesDataImportConstants.GROUP_ID_1 == groupId) {
				OneDbImport oneImport = new OneDbImport(sesUserInfo,
						dbAttr.get(0), dataSql);
				result = oneImport.start();
			} else {
				String dsAlias = dataSql.getPrimarySql().getDrAlias();
				int type = 0;
				for (SesDataSourceInfo db : dbAttr) {
					if (db.getType() == SesDataImportConstants.DBS_DB_TYPE) {
						if (db.getAuthAddr() == null
								|| db.getAuthAddr().length() == 0) {
							db.setAuthAddr(ConfUtil
									.getProperty("AUTH_ADDR_URL") + "/auth");
						}
					}
					if (db.getAlias().equals(dsAlias)) {
						userName = db.getUser();
						password = db.getServicePwd();
						serviceId = db.getServiceId();
						// 主表是查询普通数据库
						type = db.getType();
					}
				}

				ImportData importData = new ImportData(userName, password,
						serviceId, authAddress, dataSql, dbAttr, sesUserInfo,
						type);
				result = importData.start();
			}
			times = System.currentTimeMillis() - begin;
			log.info("-------all------" + times);
			res.put("CODE", "000");
			if (result.getTotalNum() == result.getSucTotal()) {
				res.put("MSG", " database totals:" + result.getTotalNum()
						+ ",ses totals:" + result.getSucTotal() + ",used time:"
						+ times + "ms.");
			} else if (result.getExcLog() != null
					&& result.getExcLog().length() > 0) {
				res.put("MSG", " database totals:" + result.getTotalNum()
						+ ",ses totals:" + result.getSucTotal() + ",used time:"
						+ times + "ms. error info:" + result.getExcLog());
			}

		} catch (Exception e) {
			res.put("CODE", "999");
			if (result != null) {
				res.put("MSG", " database totals:" + result.getTotalNum()
						+ ",ses totals:" + result.getSucTotal() + ",used time:"
						+ times + "ms. error info:" + e.getMessage());
			} else {
				res.put("MSG", e.getMessage());
			}
			log.error("--importData exception--", e);
		}

		return GsonUtil.objToGson(res);
	}

	private String startClearData(HttpServletRequest request) {
		Map<String, String> res = new HashMap<String, String>();
		long times = 0l;
		try {
			Map<String, String> userInfo = ParamUtil.getUser(request);
			long begin = System.currentTimeMillis();
			ISearchClient is = SesUtil.getSESInstance(userInfo.get("pId"),
					userInfo.get("sid"), userInfo.get("pwd"),
					ConfUtil.getProperty("AUTH_ADDR_URL") + "/auth");
			is.clean();
			is.close();
			times = System.currentTimeMillis() - begin;
			log.info("-------all------" + times);
			res.put("CODE", "000");
			res.put("MSG", " The SES data are cleaned successfully! </br> used time:"
					+ times + "ms.");

		} catch (Exception e) {
			res.put("CODE", "999");
			res.put("MSG", " The SES data clean failed!");
			log.error("--importData exception--", e);
		}

		return GsonUtil.objToGson(res);
	}

}
