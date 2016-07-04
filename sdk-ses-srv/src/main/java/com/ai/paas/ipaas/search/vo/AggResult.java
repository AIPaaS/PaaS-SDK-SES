package com.ai.paas.ipaas.search.vo;

import java.util.ArrayList;
import java.util.List;

public class AggResult {
	private String key = null;
	private long count = 0;
	// 开始嵌套
	private List<AggResult> subResult = null;

	public AggResult(String key, long count) {
		this.key = key;
		this.count = count;

	}

	public void addSubAgg(AggResult subAgg) {
		if (null == subResult) {
			synchronized (subResult) {
				if (null == subResult)
					subResult = new ArrayList<>();
			}
		}
		subResult.add(subAgg);
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public List<AggResult> getSubResult() {
		return subResult;
	}

	public void setSubResult(List<AggResult> subResult) {
		this.subResult = subResult;
	}

}
