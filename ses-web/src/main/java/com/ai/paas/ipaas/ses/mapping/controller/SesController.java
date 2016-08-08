package com.ai.paas.ipaas.ses.mapping.controller;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ai.paas.ipaas.ses.common.constants.SesConstants;
import com.ai.paas.ipaas.ses.dataimport.util.ConfUtil;
import com.ai.paas.ipaas.ses.dataimport.util.ParamUtil;
import com.ai.paas.ipaas.ses.manage.rest.interfaces.IRPCIndexMapping;
import com.ai.paas.ipaas.ses.mapping.model.SesMappingApply;
import com.ai.paas.ipaas.util.HttpUtil;
import com.ai.paas.ipaas.vo.ses.SesUserMapping;
import com.google.gson.Gson;

@Controller
@RequestMapping(value = "/ses")
public class SesController {
	private static final transient Logger LOGGER = LoggerFactory
			.getLogger(SesController.class);
	@Autowired
	IRPCIndexMapping indexMappingSRV;

	@RequestMapping(value = "/mapping")
	public String mapping(ModelMap model, HttpServletRequest request) {
		String userId = ParamUtil.getUser(request).get("userId");
		String serviceId = ParamUtil.getUser(request).get("sid");
		model.addAttribute("serviceId", serviceId);
		String mappingKey = "mapping";
		try {
			SesUserMapping mapping = indexMappingSRV.loadMapping(userId,
					serviceId);
			model.addAttribute("indexDisplay", mapping.getIndexDisplay());
			model.addAttribute("updateTime", mapping.getUpdateTime());
		} catch (Throwable e) {
			model.addAttribute(mappingKey, "{}");
			LOGGER.info(SesConstants.EXPECT_ONE_RECORD_FAIL, e);
		}
		return "/mapping/mapping";
	}

	@RequestMapping(value = "/assembleMapping")
	public String assembleMapping(ModelMap model, HttpServletRequest request) {
		String userId = ParamUtil.getUser(request).get("userId");
		String serviceId = ParamUtil.getUser(request).get("sid");
		model.addAttribute("serviceId", serviceId);
		String mappingKey = "mapping";
		try {

			SesUserMapping mapping = indexMappingSRV.loadMapping(userId,
					serviceId);
			model.addAttribute(mappingKey, mapping.getMapping());
			model.addAttribute("indexDisplay", mapping.getIndexDisplay());
			model.addAttribute("pk", mapping.getPk());
			model.addAttribute("copyto", mapping.getCopyTo());
		} catch (Throwable e) {
			model.addAttribute(mappingKey, "{}");
			LOGGER.info(SesConstants.EXPECT_ONE_RECORD_FAIL, e);
		}
		return "/mapping/assembleMapping";
	}

	@RequestMapping(value = "/mappingSuccess")
	public String mappingSuccess() {
		return "/mapping/mappingSuccess";
	}

	@SuppressWarnings("unchecked")
	@ResponseBody
	@RequestMapping(value = "/saveMapping")
	public String saveMapping(HttpServletRequest request) {

		String mapping = request.getParameter("mapping");
		String userId = ParamUtil.getUser(request).get("userId");
		String serviceId = ParamUtil.getUser(request).get("sid");
		String copyto = request.getParameter("copyto");
		String assembledJson = replaceJsonForNeed(request);
		//
		int indexName = Math.abs((userId + serviceId).hashCode());
		Map<String, Object> properties = new HashMap<String, Object>();
		properties = new Gson().fromJson(assembledJson, properties.getClass());
		Map<String, Object> mappingMap = new HashMap<String, Object>();
		if (assembledJson.indexOf("nGram_analyzer") >= 0) {
			Map<String, Object> allProps = new HashMap<String, Object>();
			allProps.put("analyzer", "nGram_analyzer");
			allProps.put("search_analyzer", "ik_max_word");
			allProps.put("term_vector", "no");
			allProps.put("store", "false");
			mappingMap.put("_all", allProps);
		}
		mappingMap.put("dynamic", "strict");

		mappingMap.put("properties", properties);
		Map<Integer, Object> indexmappingMap = new HashMap<Integer, Object>();
		indexmappingMap.put(indexName, mappingMap);

		SesMappingApply apply = new SesMappingApply();
		apply.setUserId(userId);
		apply.setServiceId(serviceId);
		apply.setIndexType(String.valueOf(indexName));
		apply.setIndexName(String.valueOf(indexName));
		apply.setMapping(new Gson().toJson(indexmappingMap));
		apply.setCopyto(copyto);
		Gson gson = new Gson();
		String json = gson.toJson(apply);
		LOGGER.debug("创建mapping json+++++++" + json, json);
		SesUserMapping userMapping = new SesUserMapping();
		userMapping.setMapping(mapping);
		userMapping.setUserId(userId);
		userMapping.setServiceId(serviceId);
		userMapping.setIndexDisplay(request.getParameter("indexDisplay"));
		userMapping.setPk(request.getParameter("pk"));
		userMapping.setUpdateTime(new Timestamp(System.currentTimeMillis()));
		userMapping.setCopyTo(copyto);
		String result = "";

		try {

			indexMappingSRV.insertMapping(userMapping);
			StringEntity dataEntity = new StringEntity(json,
					ContentType.APPLICATION_JSON);
			result = HttpUtil.doPost(ConfUtil.getProperty("SES_MAPPING_URL"),
					dataEntity);

		} catch (Exception e) {
			LOGGER.error(SesConstants.ExecResult.FAIL, e);
			return "{\"resultCode\":\"99999\",\"MSG\":\"" + e + "\"}";
		}
		return result;
	}

	private String replaceJsonForNeed(HttpServletRequest request) {
		String assembledJson = request.getParameter("assembledJson");
		// 这里有可能需要聚合，也可能不需要，每个字段都要单独处理
		// 怎么拆分字段,按字符串处理吧，转换成json对象也很麻烦,因为可能嵌套
		// 先处理聚合
		int pos = -1;
		int start = 0;
		pos = assembledJson.indexOf("\"agged\":true");
		while (pos >= 0) {
			// 先截断
			String pre = assembledJson.substring(start, pos);
			String pro = assembledJson.substring(pos
					+ "\"agged\":true".length());
			// 从pre里面找到type,然后拼接
			int typePos = pre.indexOf("\"type\"");
			// 还有索引也得替换掉了
			String type = pre.substring(typePos, pre.indexOf(",", typePos));
			pre = pre.replaceAll("\"analyze\":true",
					"\"analyzer\":\"nGram_analyzer"
							+ "\",\"searchAnalyzer\":\"ik_max_word" + "\"");
			assembledJson = pre + " \"fields\": {" + "     \"raw\": {" + type
					+ "," + "        \"index\": \"not_analyzed\"" + "     }}"
					+ pro;

			pos = assembledJson.indexOf("\"agged\":true");
		}
		assembledJson = assembledJson.replaceAll("\"analyze\":true",
				"\"analyzer\":\"ik_max_word"
						+ "\",\"searchAnalyzer\":\"ik_max_word" + "\"");
		assembledJson = assembledJson.replaceAll("\"analyze\":false,", "");
		assembledJson = assembledJson.replaceAll("\"index\":true,", "");
		assembledJson = assembledJson.replaceAll(",\"agged\":false", "");
		assembledJson = assembledJson.replaceAll("\"index\":false",
				"\"index\":no");
		return assembledJson;
	}

	public static void main(String[] args) {
		String json = "{\"newKey\":{\"type\":\"string\",\"index\":true,\"analyze\":false,\"store\":true},\"newKey1\":{\"field-name\":{\"type\":\"integer\",\"index\":true,\"analyze\":true,\"store\":true},\"copy_to\":[\"ee\"]},\"ee\":{\"type\":\"string\"}}";
		json = json.replaceAll("\"analyze\":true",
				"\"indexAnalyzer\":\"ik\",\"searchAnalyzer\":\"ik\"");
		json = json.replaceAll("\"analyze\":false,", "");
		json = json.replaceAll("\"index\":true,", "");
		json = json.replaceAll("\"index\":false", "\"index\":no");
		System.out.println(json);
	}

}
