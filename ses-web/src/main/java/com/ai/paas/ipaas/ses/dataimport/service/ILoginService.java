package com.ai.paas.ipaas.ses.dataimport.service;

import java.util.Map;

import javax.servlet.http.HttpSession;

public interface ILoginService {
	@SuppressWarnings("rawtypes")
	String login(HttpSession session,Map map);
}
