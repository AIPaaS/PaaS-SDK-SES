package com.ai.paas.ipaas.search.vo;

import java.io.Serializable;
import java.util.List;
import java.util.Set;


public class SearchfieldVo implements Serializable {

	private static final long serialVersionUID = 9152243091714512036L;
	private String filedName;
	private List<String> filedValue;
//	private String searchLogic;
	private SearchOption option;
	public String getFiledName() {
		return filedName;
	}
	public void setFiledName(String filedName) {
		this.filedName = filedName;
	}
	public List<String> getFiledValue() {
		return filedValue;
	}
	public void setFiledValue(List<String> filedValue) {
		this.filedValue = filedValue;
	}
	public SearchOption getOption() {
		return option;
	}
	public void setOption(SearchOption option) {
		this.option = option;
	}
	

}
