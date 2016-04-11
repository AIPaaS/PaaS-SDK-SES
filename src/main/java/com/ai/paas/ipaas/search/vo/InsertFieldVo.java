package com.ai.paas.ipaas.search.vo;

import java.io.Serializable;


public class InsertFieldVo implements Serializable {

	private static final long serialVersionUID = 9152243091714512036L;
	private String filedName;
	private Object filedValue;
	private FiledType fileType=FiledType.string;
//	private String searchLogic;
	

	
	public enum FiledType {
	      /*按照quert_string搜索，搜索非词组时候使用*/
	      string,
	      /*按照区间搜索*/
	      completion
	  }



	public String getFiledName() {
		return filedName;
	}



	public void setFiledName(String filedName) {
		this.filedName = filedName;
	}



	public Object getFiledValue() {
		return filedValue;
	}



	public void setFiledValue(Object filedValue) {
		this.filedValue = filedValue;
	}



	public FiledType getFileType() {
		return fileType;
	}



	public void setFileType(FiledType fileType) {
		this.fileType = fileType;
	}
	
	
	
}
