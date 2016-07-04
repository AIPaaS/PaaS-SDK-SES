package com.ai.paas.ipaas.search.vo;

public class Sort {
	public enum SortOrder {
		ASC, DESC
	}

	private String sortBy;
	private SortOrder order = SortOrder.DESC;

	public Sort(String sortBy, SortOrder order) {
		this.sortBy = sortBy;
		this.order = order;
	}

	public Sort(String sortBy) {
		this(sortBy, SortOrder.DESC);
	}

	public String getSortBy() {
		return sortBy;
	}

	public void setSortBy(String sortBy) {
		this.sortBy = sortBy;
	}

	public SortOrder getOrder() {
		return order;
	}

	public void setOrder(SortOrder order) {
		this.order = order;
	}

}