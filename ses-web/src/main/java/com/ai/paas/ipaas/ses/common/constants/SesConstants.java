package com.ai.paas.ipaas.ses.common.constants;

import com.ai.paas.ipaas.PaaSConstant;

public class SesConstants extends PaaSConstant{
	
	public static final String EXPECT_ONE_RECORD_FAIL = "Expect just one record but got nothing or too many results...";
	public static final String RECORD_EXISTS = "record already exists...";
	public static final String SAVE_TO_DB_ERROR = "save to db error...";
	public static final String SPLITER_COLON = ":";
	public static final String SPLITER_COMMA = ",";
	
	private SesConstants(){
		
	}
	
	
	
	/**
	 * 认证返回结果
	 */
	public static class ExecResult{
		public static final  String SUCCESS = "000000"; //成功
		public static final  String FAIL = "999999"; //失败
		private ExecResult(){
			
		}
	}
}
