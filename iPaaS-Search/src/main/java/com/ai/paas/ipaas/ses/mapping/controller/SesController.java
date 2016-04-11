package com.ai.paas.ipaas.ses.mapping.controller;


import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ai.paas.ipaas.PaasException;
import com.ai.paas.ipaas.ses.common.constants.SesConstants;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserMapping;
import com.ai.paas.ipaas.ses.dataimport.util.ConfUtil;
import com.ai.paas.ipaas.ses.dataimport.util.HttpClientUtil;
import com.ai.paas.ipaas.ses.dataimport.util.ParamUtil;
import com.ai.paas.ipaas.ses.mapping.model.SesMappingApply;
import com.ai.paas.ipaas.ses.mapping.service.IMappingService;
import com.google.gson.Gson;

@Controller
@RequestMapping(value = "/ses")
public class SesController {
	private static final transient Logger LOGGER = LoggerFactory.getLogger(SesController.class);
	@Autowired
	IMappingService iMappingService;
	
	@RequestMapping(value = "/mapping")
	public String mapping(ModelMap model,HttpServletRequest request) {
		String userId =  ParamUtil.getUser(request).get("userId");
		String serviceId =  ParamUtil.getUser(request).get("sid");
		model.addAttribute("serviceId", serviceId);
		String mappingKey = "mapping";
		try {
			SesUserMapping mapping = iMappingService.loadMapping(userId, serviceId);
			model.addAttribute("indexDisplay", mapping.getIndexDisplay());
			model.addAttribute("updateTime", mapping.getUpdateTime());
		} catch (PaasException e) {
			model.addAttribute(mappingKey, "{}");
			LOGGER.info(SesConstants.EXPECT_ONE_RECORD_FAIL, e);
		}
		return "/mapping/mapping";
	}
	@RequestMapping(value = "/assembleMapping")
	public String assembleMapping(ModelMap model,HttpServletRequest request) {
		String userId =  ParamUtil.getUser(request).get("userId");
		String serviceId =  ParamUtil.getUser(request).get("sid");
		model.addAttribute("serviceId", serviceId);
		String mappingKey = "mapping";
		try {
			
			SesUserMapping mapping = iMappingService.loadMapping(userId, serviceId);
			model.addAttribute(mappingKey, mapping.getMapping());
			model.addAttribute("indexDisplay", mapping.getIndexDisplay());
			model.addAttribute("pk", mapping.getPk());
			model.addAttribute("copyto", mapping.getCopyTo());
		} catch (PaasException e) {
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
		
		String mapping =  request.getParameter("mapping");
		String userId = ParamUtil.getUser(request).get("userId");
		String serviceId =  ParamUtil.getUser(request).get("sid");
		String pk =  request.getParameter("pk");
		String copyto = request.getParameter("copyto");
		String assembledJson = replaceJsonForNeed(request);
		
		int indexName =Math.abs((userId + serviceId).hashCode());
		Map<String, Object> properties = new HashMap<String, Object>();
		properties =new  Gson().fromJson(assembledJson, properties.getClass());
		Map<String, Object> mappingMap = new HashMap<String, Object>();
		mappingMap.put("dynamic", "strict");
		
		Map<String, Object> idProperties = new HashMap<String, Object>();
		idProperties.put("path", pk);
		mappingMap.put("_id", idProperties);
		mappingMap.put("properties",properties);
		Map<Integer, Object> indexmappingMap = new HashMap<Integer, Object>();
		indexmappingMap.put(indexName, mappingMap);
		
		
		SesMappingApply apply = new SesMappingApply();
		apply.setUserId(userId);
		apply.setServiceId(serviceId);
		apply.setIndexType(String.valueOf(indexName));
		apply.setIndexName(String.valueOf(indexName));
		apply.setMapping(new  Gson().toJson(indexmappingMap));
		apply.setCopyto(copyto);
		
		String json = new  Gson().toJson(apply);
		LOGGER.debug("创建mapping json+++++++"+json,json);
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
			
			iMappingService.insertMapping(userMapping);
			result = HttpClientUtil.sendPostRequest(ConfUtil.getProperty("SES_MAPPING_URL"),json);
			
		} catch (Exception e) {
			LOGGER.error(SesConstants.ExecResult.FAIL, e);
			return "{\"resultCode\":\"99999\",\"MSG\":\""+e+"\"}";
		}
		return result;
	}
	private String replaceJsonForNeed(HttpServletRequest request) {
		String userId = ParamUtil.getUser(request).get("userId");
		String serviceId =  ParamUtil.getUser(request).get("sid");
		String assembledJson = request.getParameter("assembledJson");
		assembledJson = assembledJson.replaceAll("\"analyze\":true", "\"indexAnalyzer\":\"ik_tt_"+userId+"_"+serviceId+"\",\"searchAnalyzer\":\"ik_tt_"+userId+"_"+serviceId+"\"");
		assembledJson = assembledJson.replaceAll("\"analyze\":false,", "");
		assembledJson = assembledJson.replaceAll("\"index\":true,", "");
		assembledJson = assembledJson.replaceAll("\"index\":false", "\"index\":no");
		return assembledJson;
	}
	public static void main(String[] args) {
		String json = "{\"newKey\":{\"type\":\"string\",\"index\":true,\"analyze\":false,\"store\":true},\"newKey1\":{\"field-name\":{\"type\":\"integer\",\"index\":true,\"analyze\":true,\"store\":true},\"copy_to\":[\"ee\"]},\"ee\":{\"type\":\"string\"}}";
		json = json.replaceAll("\"analyze\":true", "\"indexAnalyzer\":\"ik\",\"searchAnalyzer\":\"ik\"");
		json = json.replaceAll("\"analyze\":false,", "");
		json = json.replaceAll("\"index\":true,", "");
		json = json.replaceAll("\"index\":false", "\"index\":no");
		System.out.println(json);
	}
	
}
