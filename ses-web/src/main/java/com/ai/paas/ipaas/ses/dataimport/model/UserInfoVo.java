package com.ai.paas.ipaas.ses.dataimport.model;

import java.io.Serializable;

/**
 * 辅助sql
 *
 */
public class UserInfoVo implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5015586402211527083L;
	
	/**登录名*/
	private String userName;
	
	/**服务id*/
	private String serviceId;
	
	/**服务密码*/
	private String servicePwd;

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getServiceId() {
		return serviceId;
	}

	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}

	public String getServicePwd() {
		return servicePwd;
	}

	public void setServicePwd(String servicePwd) {
		this.servicePwd = servicePwd;
	}
	

}
