package com.ai.paas.ipaas.search.vo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SearchCriteria implements Serializable {

	private static final long serialVersionUID = 9152243091714512036L;
	private String field;
	private List<String> values;
	private SearchOption option;

	private List<SearchCriteria> subCriterias;

	public SearchCriteria() {
		values = new ArrayList<String>();
		subCriterias = new ArrayList<SearchCriteria>();
		this.option = new SearchOption();
	}

	public SearchCriteria(String field, SearchOption searchOption) {
		this();
		this.field = field;
		this.option = searchOption;
	}

	public SearchCriteria(String field, String value, SearchOption searchOption) {
		this(field, searchOption);
		this.addFieldValue(value);
	}

	public String getField() {
		return field;
	}

	public String getFormatField() {
		if (null != field)
			return field.toLowerCase();
		else
			return null;
	}

	public void setField(String Field) {
		this.field = Field;
	}

	public List<String> getFieldValue() {
		return values;
	}

	public void setFieldValue(List<String> filedValue) {
		this.values.addAll(filedValue);
	}

	public SearchOption getOption() {
		return option;
	}

	public void setOption(SearchOption option) {
		this.option = option;
	}

	public SearchCriteria addFieldValue(String value) {
		this.values.add(value);
		return this;
	}

	public SearchCriteria addSubCriteria(SearchCriteria subCriteria) {
		subCriterias.add(subCriteria);
		return this;
	}

	public boolean hasSubCriteria() {
		return subCriterias.size() > 0 ? true : false;
	}

	public List<SearchCriteria> getSubCriterias() {
		return subCriterias;
	}
}
