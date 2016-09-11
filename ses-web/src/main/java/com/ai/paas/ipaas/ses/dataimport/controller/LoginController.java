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
	
	
	@SuppressWarnings("unchecked")
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
	
	@RequestMapping(value = "/portalLogin")
	public String portalLogin(HttpServletRequest request,HttpServletResponse response) {
		
		String urlInfo = request.getParameter(ConstantsForSession.LoginSession.URL_INFO);
		request.setAttribute("urlInfo", urlInfo);
		session.setAttribute(ConstantsForSession.LoginSession.USER_INFO, null);
		session.invalidate();
		
		Map<String, Object> modelMap = new HashMap<String, Object>();
		String userName = request.getParameter("userName");
		String serviceId = request.getParameter("serviceId");
		String servicePwd = request.getParameter("servicePwd");
		modelMap.put("userName", userName);
		modelMap.put("sid", serviceId);
		modelMap.put("pwd", servicePwd);
		//解决跨域问题=======start
		response.setHeader("Access-Control-Allow-Origin", "*");		
		response.setHeader("Access-Control-Allow-Credentials", "true");	
		response.setHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
		response.setHeader("Access-Control-Allow-Headers", "Origin, No-Cache, X-Requested-With, If-Modified-Since, Pragma, Last-Modified, Cache-Control, Expires, Content-Type, X-E4M-With");
		response.setHeader("Content-Type", "text/html; charset=utf-8");
		//解决跨域问题=======end
		try {
			String res = iLoginService.login(request.getSession(),modelMap);
			//return returnMap;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		//跳转到首页
		return "../index";
	}
	
	
}
