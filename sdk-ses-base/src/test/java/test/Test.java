package test;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

import com.ai.paas.ipaas.search.vo.Sort;
import com.ai.paas.ipaas.search.vo.Sort.SortOrder;

public class Test {

	public static void main(String[] args) throws Exception {
		String json = "{\"sorts\":[{\"order\":\"1\",\"sortBy\":\"exttaskid\"}]}";
		String json1 = "{\"order\":\"1\",\"sortBy\":\"exttaskid\"}";
		String json2 = "{\"searchCriterias\":[{\"field\":\"actprokey\",\"fieldValue\":[\"leave\"],\"option\":{\"boost\":1.0,\"dataFilter\":\"1\",\"highlight\":false,\"queryStringPrecision\":\"90%\",\"searchLogic\":\"1\",\"searchType\":\"1\",\"termOperator\":\"1\"},\"subCriterias\":[]},{\"field\":\"busititle\",\"fieldValue\":[\"fuck you bitch\"],\"option\":{\"boost\":1.0,\"dataFilter\":\"1\",\"highlight\":false,\"queryStringPrecision\":\"90%\",\"searchLogic\":\"1\",\"searchType\":\"1\",\"termOperator\":\"1\"},\"subCriterias\":[]}]}";
		ObjectMapper mapper = new ObjectMapper();
		// JSON from String to Object
		Sort sort = mapper.readValue(json1, Sort.class);
		System.out.println(sort.getOrder());
		TestVO obj = mapper.readValue(json, TestVO.class);
		System.out.println(obj.getSorts().get(0).getSortBy());
		TestVo1 vo1 = mapper.readValue(json2, TestVo1.class);
		System.out.println(vo1.getSearchCriterias().get(0).getOption().getQueryStringPrecision());
		List<Sort> sorts = new ArrayList<>();
		Sort sort1 = new Sort("test", SortOrder.DESC);
		sorts.add(sort1);
		TestVO testVO = new TestVO();
		testVO.setSorts(sorts);
		System.out.println(mapper.writeValueAsString(testVO));
		
		
		String test="dsfsdfsdfds^sdfsdf^dsfsdf";
		System.out.println(test.replaceAll("\\^", ""));
		
	}

}
