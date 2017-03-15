package com.ai.paas.ipaas.search.vo;

import java.io.Serializable;

import com.ai.paas.ipaas.util.StringUtil;

public class Sort implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7214829498157852617L;

	public enum SortOrder {
		ASC(1), DESC(2);
		private final int value;

		SortOrder(int v) {
			value = v;
		}

		@org.codehaus.jackson.annotate.JsonValue
		public int value() {
			return value;
		}

		@org.codehaus.jackson.annotate.JsonCreator
		public static SortOrder fromValue(int typeCode) {
			for (SortOrder c : SortOrder.values()) {
				if (c.value == typeCode) {
					return c;
				}
			}
			throw new IllegalArgumentException("Invalid Status type code: " + typeCode);

		}
	}

	private String sortBy;
	private SortOrder order = SortOrder.DESC;

	public Sort() {

	}

	public Sort(String order, String sortBy) {
		this.sortBy = sortBy;
		if (!StringUtil.isBlank(order) && "ASC".equalsIgnoreCase(order))
			this.order = SortOrder.ASC;
	}

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