package com.ai.paas.ipaas.ses.overview.controller;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;

import com.ai.paas.ipaas.PaasException;
import com.ai.paas.ipaas.ses.common.constants.SesConstants;
import com.ai.paas.ipaas.ses.dataimport.util.ParamUtil;
import com.ai.paas.ipaas.ses.manage.rest.interfaces.IRPCSesUserInst;
import com.ai.paas.ipaas.vo.ses.SesUserInstance;
import com.ai.paas.ipaas.ses.service.interfaces.ISesManage;

@Controller
@RequestMapping(value = "/overview")
public class OverViewController {
	private static final transient Logger LOGGER = LoggerFactory
			.getLogger(OverViewController.class);
	@Autowired
	IRPCSesUserInst sesUserInst;

	@RequestMapping(value = "/overview")
	public String mapping(ModelMap model, HttpServletRequest request) {
		String userId = ParamUtil.getUser(request).get("userId");
		String serviceId = ParamUtil.getUser(request).get("sid");
		model.addAttribute("userId", userId);
		model.addAttribute("serviceId", serviceId);
		try {
			SesUserInstance ins = sesUserInst.queryInst(userId, serviceId);
			//*****追加hostip的内外网地址对应 start******
			String hostIps=ISesManage.getHostIp();
			String hostIp;
			ArrayList<String> hostList =hostIps.split(SesConstants.SPLITER_COMMA);
			for(int i=0;i<hostList.length();i++){
				if (hostList[i].contains(ins.getHostIp())){
					hostIp = hostList[i].split(SesConstants.SPLITER_COLON)[1];
					break;
				}
			}
			//*****追加hostip的内外网地址对应 end******
			String addr = "http://" + hostIp + ":" + ins.getSesPort()
					+ "/_plugin/head/";
			model.addAttribute("addr", addr);
		} catch (PaasException e) {
			LOGGER.info(SesConstants.EXPECT_ONE_RECORD_FAIL, e);
		}
		LOGGER.info("going to page overview");
		return "/overview";
	}
}
