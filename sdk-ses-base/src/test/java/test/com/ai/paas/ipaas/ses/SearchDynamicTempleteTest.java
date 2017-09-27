package test.com.ai.paas.ipaas.ses;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.ai.paas.ipaas.search.ISearchClient;
import com.ai.paas.ipaas.search.SearchCmpClientFactory;
import com.ai.paas.ipaas.search.common.DynamicMatchOption;
import com.ai.paas.ipaas.search.common.TypeGetter;
import com.ai.paas.ipaas.search.vo.Result;
import com.ai.paas.ipaas.search.vo.SearchCriteria;
import com.ai.paas.ipaas.search.vo.SearchOption;
import com.ai.paas.ipaas.search.vo.SearchOption.TermOperator;

public class SearchDynamicTempleteTest {

	static ISearchClient client = null;
	static String indexName = "user";
	static String mapping = null;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		// String hosts = "10.1.235.22:9300,10.1.235.23:9300,10.1.235.24:9300";
		String hosts = "127.0.0.1:9300";
		mapping = "  {"
				+ "     	\"address\" :  {\"type\" : \"string\", \"store\" : \"yes\",\"analyzer\": \"ik_max_word\"},"
				+ "     	\"dxf\" :  {\"type\" : \"string\", \"store\" : \"yes\",\"index\": \"not_analyzed\"}"
				+ "     }";
		String id = "id";
		client = SearchCmpClientFactory.getSearchClient(hosts, indexName, id);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		client = null;
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testBulkInsertListOfT() {
		List<Bar<Foo>> datas = new ArrayList<>();
		Bar<Foo> bar1 = new Bar<>();
		Foo foo = new Foo("11", "西城张三", 45, "西城张三某人");
		bar1.setAddress("西城区");
		bar1.setT(foo);

		Bar<Foo> bar2 = new Bar<>();
		Foo foo2 = new Foo("12", "东城王五", 45, "东城王五某人");
		bar2.setAddress("东城区");
		bar2.setT(foo2);

		Bar<Foo> bar3 = new Bar<>();
		Foo foo3 = new Foo("11", "海淀李四", 45, "海淀李四某人");
		bar3.setAddress("海淀区");
		bar3.setT(foo3);
		datas.add(bar1);
		datas.add(bar2);
		datas.add(bar3);
		assertTrue(client.bulkInsert(datas));
	}

	@Test
	public void testSearchTerm() {
		List<SearchCriteria> searchCriterias = new ArrayList<>();
		SearchCriteria searchCriteria = new SearchCriteria("address", "东城区",
				new SearchOption(SearchOption.SearchLogic.must, SearchOption.SearchType.term, TermOperator.AND));
		searchCriterias.add(searchCriteria);
		TypeGetter<Bar<Foo>> typeGetter = new TypeGetter<Bar<Foo>>() {
		};
		Result<Bar<Foo>> result = client.search(searchCriterias, 0, 10, null, typeGetter);
		assertTrue(result.getContents().get(0).getAddress().equals("东城区"));
	}
	
	@Test
	public void testAddMapping() {
		if (client.existIndex(indexName))
			client.deleteIndex(indexName);
		client.createIndex(indexName, 3, 0);
		List<DynamicMatchOption> matchs = new ArrayList<>();
		// 以id开头的都不分词
		DynamicMatchOption matchNotAnalyzed = new DynamicMatchOption("NotAnalyzed",
				DynamicMatchOption.MatchType.PATTERN, "^\\\\S*?(id)$", null, false);
		matchs.add(matchNotAnalyzed);
		// 以name\title\content的进行分词
		DynamicMatchOption matchAnalyzed = new DynamicMatchOption("Analyzed", DynamicMatchOption.MatchType.PATTERN,
				"^\\\\S*?[(name)|(content)|(desc)]\\\\S*?$", null, true);
		// 默认不进行分词
		matchs.add(matchAnalyzed);
		DynamicMatchOption defaultNotAnalyzed = new DynamicMatchOption("default", DynamicMatchOption.MatchType.SIMPLE,
				"*", null, false);
		matchs.add(defaultNotAnalyzed);
		assertTrue(client.addMapping(indexName, indexName, mapping, matchs));
	}

}
