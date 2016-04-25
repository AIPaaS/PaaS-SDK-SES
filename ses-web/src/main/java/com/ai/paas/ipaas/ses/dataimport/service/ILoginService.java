package com.ai.paas.ipaas.ses.dataimport.service;

import java.util.Map;

import javax.servlet.http.HttpSession;

public interface ILoginService {
	String login(HttpSession session,Map map);
}
