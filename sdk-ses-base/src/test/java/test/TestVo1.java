package test;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.annotate.JsonDeserialize;

import com.ai.paas.ipaas.search.vo.SearchCriteria;

public class TestVo1 {
	@JsonDeserialize(as = ArrayList.class, contentAs = SearchCriteria.class)
	private List<SearchCriteria> searchCriterias = null;

	public List<SearchCriteria> getSearchCriterias() {
		return searchCriterias;
	}

	public void setSearchCriterias(List<SearchCriteria> searchCriterias) {
		this.searchCriterias = searchCriterias;
	}

}
