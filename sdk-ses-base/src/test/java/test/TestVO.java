package test;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.annotate.JsonDeserialize;

import com.ai.paas.ipaas.search.vo.Sort;


public class TestVO {
	@JsonDeserialize(as = ArrayList.class, contentAs = Sort.class)
	private List<Sort> sorts = null;

	public List<Sort> getSorts() {
		return sorts;
	}

	public void setSorts(List<Sort> sorts) {
		this.sorts = sorts;
	}

}
