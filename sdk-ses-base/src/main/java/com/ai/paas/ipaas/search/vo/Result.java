package com.ai.paas.ipaas.search.vo;

import java.util.ArrayList;
import java.util.List;

public class Result<T> {
	// 搜索的结果集
	private List<T> contents = new ArrayList<T>();

	private List<AggResult> aggs = new ArrayList<>();
	// 总数
	private long count;

	private String resultCode;

	public String getResultCode() {
		return resultCode;
	}

	public void setResultCode(String resultCode) {
		this.resultCode = resultCode;
	}

	public List<T> getContents() {
		return contents;
	}

	public void setContents(List<T> contents) {
		this.contents = contents;
	}

	public void addContent(T t) {
		contents.add(t);
	}

	public long getCount() {
		return count;
	}

	public void setCounts(long count) {
		this.count = count;
	}

	public List<AggResult> getAggs() {
		return aggs;
	}

	public void setAggs(List<AggResult> aggs) {
		this.aggs = aggs;
	}

	public void addAgg(AggResult agg) {
		aggs.add(agg);
	}

}
