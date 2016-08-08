package com.ai.paas.ipaas.ses.dataimport.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.vo.ses.SesDataSourceInfo;

public class ParamUtil {
	private static final transient Logger LOGGER = LoggerFactory
			.getLogger(ParamUtil.class);

	private ParamUtil() {

	}

	public static List<SesDataSourceInfo> getDs(HttpServletRequest request,
			String flag) {
		List<SesDataSourceInfo> dataList = new ArrayList<>();

		String groupId = request.getParameter("groupId");
		String param = request.getParameter("param");
		if ((groupId == null || groupId.length() == 0)
				&& (param == null || param.length() == 0)) {
			return dataList;
		}
		LOGGER.info("Start to get data source!");
		if ("delete".equals(flag)) {
			SesDataSourceInfo datas = new SesDataSourceInfo();
			datas.setGroupId(Integer.valueOf(groupId));
			datas.setAlias(request.getParameter("alias"));
			datas.setuId((request.getParameter("uId") == null || request
					.getParameter("uId").equals("")) ? 0 : Integer
					.valueOf(request.getParameter("uId")));
			dataList.add(datas);
			return dataList;
		}

		if (groupId != null
				&& groupId.length() > 0
				&& (Integer.valueOf(groupId) == SesDataImportConstants.GROUP_ID_1)
				|| Integer.valueOf(groupId) == SesDataImportConstants.GROUP_ID_2) {
			SesDataSourceInfo datas = new SesDataSourceInfo();
			datas.setGroupId(Integer.valueOf(groupId));
			String type = request.getParameter("type");
			if (type != null
					&& !"".equals(type)
					&& Integer.valueOf(type) == SesDataImportConstants.DBS_DB_TYPE) {
				// 数据源是DBS
				datas.setUser(request.getParameter("user"));
				datas.setServiceId(request.getParameter("serviceId"));
				datas.setServicePwd(request.getParameter("servicePwd"));
				datas.setVsql(request.getParameter("vsql"));
				datas.setType(SesDataImportConstants.DBS_DB_TYPE);
				datas.setAlias(request.getParameter("user").split("@")[0]
						+ SesDataImportConstants.SPLIT_STR
						+ request.getParameter("serviceId"));
			} else {
				datas.setType(SesDataImportConstants.COMMON_DB_TYPE);
				datas.setIp(request.getParameter("ip"));
				datas.setPort(Integer.valueOf(request.getParameter("port")));
				datas.setSid(request.getParameter("sid"));
				datas.setUsername(request.getParameter("username"));
				datas.setPwd(request.getParameter("pwd"));
				datas.setDatabase(Integer.valueOf(request
						.getParameter("database")));
				datas.setId((request.getParameter("id") == null || request
						.getParameter("id").equals("")) ? 0 : Integer
						.valueOf(request.getParameter("id")));
				datas.setAlias(request.getParameter("ip")
						+ SesDataImportConstants.SPLIT_STR
						+ request.getParameter("port")
						+ SesDataImportConstants.SPLIT_STR
						+ request.getParameter("sid"));
			}
			datas.setuId((request.getParameter("uId") == null || request
					.getParameter("uId").equals("")) ? 0 : Integer
					.valueOf(request.getParameter("uId")));
			if (Integer.valueOf(groupId) == SesDataImportConstants.GROUP_ID_2)
				datas.setOverwrite(request.getParameter("overwrite") == null
						|| request.getParameter("overwrite").equals("") ? false
						: Boolean.valueOf(request.getParameter("overwrite")));

			dataList.add(datas);
		}
		return dataList;

	}

	@SuppressWarnings("unchecked")
	public static Map<String, String> getUser(HttpServletRequest request) {
		Map<String, String> user = new HashMap<String, String>();
		Object obj = request.getSession().getAttribute(
				SesDataImportConstants.WEB_USER);
		if (obj != null)
			user = (HashMap<String, String>) obj;
		return user;

	}

	public static String fillStringByArgs(String str, String[] arr) {
		Matcher m = Pattern.compile("\\{(\\d)\\}").matcher(str);
		while (m.find()) {
			str = str.replace(m.group(), arr[Integer.parseInt(m.group(1))]);
		}
		return str;
	}

	public static String getDsInfo(List<SesDataSourceInfo> dbAttrs) {
		if (dbAttrs == null || dbAttrs.size() == 0)
			return null;

		StringBuffer sbf = new StringBuffer();
		for (SesDataSourceInfo db : dbAttrs) {
			sbf.append("<option value='")
					.append(db.getAlias().replace(".", "")).append("'>")
					.append(db.getAlias()).append("</option>");
		}
		return sbf.toString();
	}

	public static String getERRORMSG(String message) {
		return "{\"CODE\":\"999\",\"MSG\":\"" + message + "\"}";
	}

	
	public static Map<String, String> getSqlInfo(HttpServletRequest request,String flag){
		Map<String, String> sqlInfo = new HashMap<String, String>();
		if ("delete".equals(flag)) {
			sqlInfo.put("groupId", request.getParameter("groupId"));
			sqlInfo.put("sql.id", request.getParameter("uId"));
			sqlInfo.put("uId", request.getParameter("uId"));
			sqlInfo.put("isPrimary", request.getParameter("isPrimary"));
			sqlInfo.put("falias", request.getParameter("falias"));
			
			return sqlInfo;
		}
		return sqlInfo;
	}
}
