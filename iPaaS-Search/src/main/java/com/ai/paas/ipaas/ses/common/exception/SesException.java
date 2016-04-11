package com.ai.paas.ipaas.ses.common.exception;


/**
 *  运行异常定义
 *
 */

public class SesException extends RuntimeException {
	private static final long serialVersionUID = -8886495803406807620L;
	private final String errCode;
	private final String errDetail;

	public SesException(String errDetail) {
		this.errDetail = errDetail;
		errCode = null;
	}

	public SesException(String errCode, String errDetail) {
		this.errCode = errCode;
		this.errDetail = errDetail;
	}

	public SesException(String errCode, Exception ex) {
		this.errCode = errCode;
		errDetail = null;
	}

	public SesException(String errCode, String errDetail, Exception ex) {
		this.errCode = errCode;
		this.errDetail = errDetail;
	}

	public String getErrCode() {
		return errCode;
	}

	public String getErrDetail() {
		return errDetail;
	}


}

