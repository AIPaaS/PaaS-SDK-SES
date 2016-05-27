package com.ai.paas.ipaas.search.vo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class SearchfieldVo implements Serializable {

    private static final long serialVersionUID = 9152243091714512036L;
    private String filedName;
    private List<String> filedValue;
    //	private String searchLogic;
    private SearchOption option;

    private List<SearchfieldVo> subSearchFieldVos;

    public SearchfieldVo() {
        filedValue = new ArrayList<String>();
        subSearchFieldVos = new ArrayList<SearchfieldVo>();
        this.option = new SearchOption();
    }

    public SearchfieldVo(String filedName, SearchOption searchOption) {
        this();
        this.filedName = filedName;
        this.option = searchOption;
    }

    public SearchfieldVo(String filedName, String value, SearchOption searchOption) {
        this(filedName, searchOption);
        this.addFieldValue(value);
    }

    public String getFiledName() {
        return filedName;
    }

    public String getFormatFieldName() {
        return filedName.toLowerCase();
    }

    public void setFiledName(String filedName) {
        this.filedName = filedName;
    }

    public List<String> getFiledValue() {
        return filedValue;
    }

    public void setFiledValue(List<String> filedValue) {
        this.filedValue.addAll(filedValue);
    }

    public SearchOption getOption() {
        return option;
    }

    public void setOption(SearchOption option) {
        this.option = option;
    }

    public SearchfieldVo addFieldValue(String value) {
        this.filedValue.add(value);
        return this;
    }


    public SearchfieldVo addSubSearchFieldVo(SearchfieldVo subFieldVo) {
        subSearchFieldVos.add(subFieldVo);
        return this;
    }


    public boolean hasSubSearchFieldVo() {
        return subSearchFieldVos.size() > 0 ? true : false;
    }

    public List<SearchfieldVo> getSubSearchFieldVo() {
        return subSearchFieldVos;
    }
}
