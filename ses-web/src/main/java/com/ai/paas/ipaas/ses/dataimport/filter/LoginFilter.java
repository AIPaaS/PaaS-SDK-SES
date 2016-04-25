package com.ai.paas.ipaas.ses.dataimport.filter;

import java.io.IOException;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.util.ParamUtil;



public class LoginFilter implements Filter{
	private static final transient Logger log = LoggerFactory.getLogger(LoginFilter.class);
	
	private static final String LOGIN_PAGE ="/login/doLogin";
	private String[] ignorePages = new String[]{};
	private String[] ignoreSuffix = new String[]{};
	
	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
		String ignoreSuffixTemp = filterConfig.getInitParameter("ignore_suffix");
		if (!StringUtils.isEmpty(ignoreSuffixTemp))
			ignoreSuffix =ignoreSuffixTemp.split(",");
		
		String ignorePagesTtemp = filterConfig.getInitParameter("ignore_pages");
		if (!StringUtils.isEmpty(ignorePages)) {
			ignorePages =ignorePagesTtemp.split(",");
		}
	}

	@Override
	public void doFilter(ServletRequest request, ServletResponse response,
			FilterChain chain) throws IOException, ServletException {
		HttpServletRequest servletRequest  = (HttpServletRequest) request;
		HttpServletResponse servletResponse = (HttpServletResponse) response;
		 // 获得用户请求的URI
		String path = servletRequest.getRequestURI();
		
		StringBuilder parametersBuff = new StringBuilder();
		Map<String, String[]> parameters = request.getParameterMap();
        if (parameters != null && parameters.size() > 0) {
        	parametersBuff.append("?");
        	for (Map.Entry<String, String[]> entry : parameters.entrySet()) {
        		String[] values = entry.getValue();
                for (int i = 0; i < values.length; i++) {
                	parametersBuff.append(entry.getKey()).append("=").append(values[i]).append("&");
                }
        	}
        }
        path = path+parametersBuff.toString();
        
		
		if (shouldFilter(servletRequest)) {
			chain.doFilter(servletRequest, servletResponse);
			return;
		}
		
		//不需要过滤的页面处理
		if (ignorePages(servletRequest)) {
			chain.doFilter(servletRequest, servletResponse);
			return;
		}
		// 从session里取用户信息
		Map<String,String> userVo  = ParamUtil.getUser(servletRequest);
		//如果用户登录信息为空，则跳转到登录页面
		if (userVo == null || StringUtils.isEmpty(userVo.get("userId"))) {
			//把要请求你的url带到登录页面，然后登录后直接跳转到之前请求页面，不放到session中
			String url = servletRequest.getContextPath()+LOGIN_PAGE+"?"+SesDataImportConstants.URL_INFO+"="+
					java.net.URLEncoder.encode(path, "UTF-8");
			servletResponse.sendRedirect(url);
			log.info("###################################sendRedirect:"+url);
		}else {
			//用户已经登录
			log.info("###################################have login already");
			chain.doFilter(servletRequest, servletResponse);
		}

	}
	
	private boolean shouldFilter(HttpServletRequest request) {
		String uri = request.getRequestURI().toLowerCase();
		for (String suffix : ignoreSuffix) {
			if (uri.endsWith(suffix))
				return true;
		}
		return false;
	}
	public boolean ignorePages(HttpServletRequest request){
		String uri = request.getRequestURI();
		for (String pageUrl: ignorePages) {
			if (uri.endsWith(pageUrl)) {
				return true ;
			}
		}
		return false;
	}
	@Override
	public void destroy() {
		ignorePages = null;
		ignoreSuffix = null;
	}

	

	
}
