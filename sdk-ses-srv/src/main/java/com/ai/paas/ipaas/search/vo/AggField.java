package com.ai.paas.ipaas.search.vo;

import java.util.List;

public class AggField {

	private String field = null;
	private List<AggField> subAggs = null;

	public AggField() {

	}

	public AggField(String field) {
		this.field = field;
	}

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public List<AggField> getSubAggs() {
		return subAggs;
	}

	public void setSubAggs(List<AggField> subAggs) {
		this.subAggs = subAggs;
	}

}
