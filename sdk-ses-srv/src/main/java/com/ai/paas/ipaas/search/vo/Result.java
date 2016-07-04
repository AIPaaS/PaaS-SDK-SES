package com.ai.paas.ipaas.search.vo;

import java.util.ArrayList;
import java.util.List;

public class Result<T> {
	// 搜索的结果集
	private List<T> searchList = new ArrayList<T>();
	// 总数
	private long count;

	private String resultCode;

	public String getResultCode() {
		return resultCode;
	}

	public void setResultCode(String resultCode) {
		this.resultCode = resultCode;
	}

	public List<T> getSearchList() {
		return searchList;
	}

	public void setSearchList(List<T> searchList) {
		this.searchList = searchList;
	}

	public void addSearchList(T t) {
		searchList.add(t);
	}

	public long getCount() {
		return count;
	}

	public void setCounts(long count) {
		this.count = count;
	}

}
