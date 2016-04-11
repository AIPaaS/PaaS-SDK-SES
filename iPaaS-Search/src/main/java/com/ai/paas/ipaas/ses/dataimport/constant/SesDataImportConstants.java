package com.ai.paas.ipaas.ses.dataimport.constant;

public class SesDataImportConstants {
	
	private SesDataImportConstants(){}
	
	/**1 单库  2 多库、DBS*/
	public static final int GROUP_ID_1=1;
	public static final int GROUP_ID_2=2;
	/**普通数据库*/
	public static final int COMMON_DB_TYPE=1;
	/**DBS数据库*/
	public static final int DBS_DB_TYPE=2;
	/**mysql数据库*/
	public static final int MYSQL_DB=1;
	/**oracle数据库*/
	public static final int ORACLE_DB=2;
	
	
	/**1对1*/
	public static final int ONE_TO_ONE_RELATION=1;
	/**1对多*/
	public static final int ONE_TO_MANY_RELATION=2;
	
	/**状态 有效*/
	public static final int STATUS=1;
	/**状态 无效*/
	public static final int IN_STATUS=2;
	
	
	/**1主表*/
	public static final int PRIMARY=1;
	/**非主表*/
	public static final int IN_PRIMARY=2;
	
	
	
	/**session中用户标识*/
	public static final String WEB_USER="SES_USER";
	
	//记录登录前访问的URL
	public static final String URL_INFO ="urlInfo";
	
	
	public static final String USER_ID_STR="userId";
	public static final String SID_STR="sid";
	public static final String SPLIT_STR="_";
	public static final String SPLIT_STR_2=",,";

	
	
}
