package com.ai.paas.ipaas.search.vo;

/**
 * Created by xin on 16-6-2.
 */
public class SortField {
    private String sortField;
    private String sortType;

    public SortField(String sortField, String sortType) {
        this.sortField = sortField;
        this.sortType = sortType;
    }


    public SortField(String sortField) {
        this(sortField, "DESC");
    }

    public String getSortField() {
        return sortField;
    }

    public void setSortField(String sortField) {
        this.sortField = sortField;
    }

    public String getSortType() {
        return sortType;
    }

    public void setSortType(String sortType) {
        this.sortType = sortType;
    }
}