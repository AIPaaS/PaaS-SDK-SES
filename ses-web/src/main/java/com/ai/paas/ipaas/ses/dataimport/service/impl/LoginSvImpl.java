package com.ai.paas.ipaas.ses.dataimport.service.impl;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.ai.paas.ipaas.ses.dataimport.constant.SesDataImportConstants;
import com.ai.paas.ipaas.ses.dataimport.service.ILoginService;
import com.ai.paas.ipaas.ses.dataimport.util.ConfUtil;
import com.ai.paas.ipaas.ses.dataimport.util.GsonUtil;
import com.ai.paas.ipaas.uac.service.UserClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;

@Service
public class LoginSvImpl implements ILoginService {

	private static transient final Logger log = LoggerFactory
			.getLogger(LoginSvImpl.class);

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public String login(HttpSession session, Map map) {
		Map res = new HashMap();
		try {
			String userName = map.get("userName").toString();
			String sid = map.get("sid").toString();
			String pwd = map.get("pwd").toString();
			String authAddr = ConfUtil.getProperty("AUTH_ADDR_URL");
			AuthDescriptor ad = new AuthDescriptor(authAddr, userName, pwd, sid);
			AuthResult authResult = UserClientFactory.getUserClient().auth(ad);
			map.put("userId", authResult.getUserId());
			// 设置用户信息
			session.removeAttribute(SesDataImportConstants.WEB_USER);
			session.setAttribute(SesDataImportConstants.WEB_USER, map);
			res.put("CODE", "1");
		} catch (Exception e) {
			log.error("user error", e);
			res.put("CODE", "0");
		}
		return GsonUtil.objToGson(res);
	}

}
