package com.ai.paas.ipaas.search.vo;

import java.io.Serializable;
import java.util.List;

import com.ai.paas.ipaas.search.vo.SearchOption.SearchLogic;


public class SearchVo implements Serializable {

	private static final long serialVersionUID = 9152243091714512036L;
	
	
	private List<SearchfieldVo> searchFieldList;
	private SearchLogic searchLogic = SearchLogic.must;
	public List<SearchfieldVo> getSearchFieldList() {
		return searchFieldList;
	}
	public void setSearchFieldList(List<SearchfieldVo> searchFieldList) {
		this.searchFieldList = searchFieldList;
	}
	public SearchLogic getSearchLogic() {
		return searchLogic;
	}
	public void setSearchLogic(SearchLogic searchLogic) {
		this.searchLogic = searchLogic;
	}
	
	
	

}
