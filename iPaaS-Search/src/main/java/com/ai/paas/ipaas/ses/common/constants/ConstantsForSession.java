package com.ai.paas.ipaas.ses.common.constants;

public class ConstantsForSession {
	
	private ConstantsForSession(){}
	
	/**
	 *登录写入的Session
	 */
	public static class LoginSession{
		
		private LoginSession(){}
		/**人员信息*/
		public static final  String USER_INFO = "userInfoVO";
		//记录登录前访问的URL
		public static final  String URL_INFO ="urlInfo";
	}
}
