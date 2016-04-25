package com.ai.paas.ipaas.ses.dataimport.controller;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.ai.paas.ipaas.ses.common.constants.ConstantsForSession;
import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.service.ILoginService;
import com.google.gson.Gson;

@Controller
@RequestMapping(value = "/login")
public class LoginController {
	private static final transient Logger log = LoggerFactory.getLogger(LoginController.class);

	@Autowired
	private ILoginService iLoginService;
	@Autowired
	protected HttpSession session;
	
	
	@RequestMapping(value = "/doLogin")
	public String doLogin(HttpServletRequest request) {
		
		String urlInfo = request.getParameter(ConstantsForSession.LoginSession.URL_INFO);
		request.setAttribute("urlInfo", urlInfo);
		session.setAttribute(ConstantsForSession.LoginSession.USER_INFO, null);
		session.invalidate();
		
		return "/login";
	}
	
	
	
	@RequestMapping(value = "/doLogout")
	public String doLogout(HttpServletRequest request) {
		
		session.removeAttribute(SesDataImportConstants.WEB_USER);
		session.setAttribute(ConstantsForSession.LoginSession.USER_INFO, null);
		session.invalidate();
		return "/login";
	}
	
	
	
	@RequestMapping(value = "/noLogin")
	public String noLogin(HttpServletRequest request) {
		
		String userName = request.getParameter("userName");
		String serviceId = request.getParameter("serviceId");
		String servicePwd = request.getParameter("servicePwd");
		request.setAttribute("userName", userName);
		request.setAttribute("sid", serviceId);
		request.setAttribute("pwd", servicePwd);
		
		session.setAttribute(ConstantsForSession.LoginSession.USER_INFO, null);
		session.invalidate();
		
		return "/noLogin";
	}
	
	
	@ResponseBody
	@RequestMapping(value = "/login")
	public Map<String, Object> login(HttpServletRequest request) {
		
		
		Gson gson = new Gson();
		Map<String, Object> modelMap = new HashMap<String, Object>();
		String userName = request.getParameter("userName");
		String serviceId = request.getParameter("serviceId");
		String servicePwd = request.getParameter("servicePwd");
		
		modelMap.put("userName", userName);
		modelMap.put("sid", serviceId);
		modelMap.put("pwd", servicePwd);
		
		Map<String, Object> returnMap =new HashMap<String, Object>();
		try {
			
			String res = iLoginService.login(request.getSession(),modelMap);
			returnMap = gson.fromJson(res, returnMap.getClass());
			
			return returnMap;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return returnMap;
	}
	
	
}
