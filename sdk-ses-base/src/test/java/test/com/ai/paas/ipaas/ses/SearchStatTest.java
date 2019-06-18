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
import com.ai.paas.ipaas.search.vo.SearchCriteria;
import com.ai.paas.ipaas.search.vo.SearchOption;
import com.ai.paas.ipaas.search.vo.SearchOption.SearchLogic;
import com.ai.paas.ipaas.search.vo.SearchOption.SearchType;
import com.ai.paas.ipaas.search.vo.StatResult;

public class SearchStatTest {
    static ISearchClient client = null;
    static String indexName = "cmc-np-index";

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String hosts = "10.12.2.144:19300,10.12.2.145:19300,10.12.2.146:19300";
        String id = "_id";
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
    public void testCount() {
        List<SearchCriteria> searchCriterias = new ArrayList<>();
        SearchCriteria subCriteria = new SearchCriteria();
        subCriteria.setOption(new SearchOption(SearchLogic.must, SearchType.range));
        subCriteria.setField("activeTime");
        subCriteria.addFieldValue("2019-05-16T17:43:43+0800");
        subCriteria.addFieldValue("2019-06-04T10:40:25+0800");
        searchCriterias.add(subCriteria);
        StatResult sr = client.count(searchCriterias, "serviceNum");
        assertTrue(sr.getCount() == 6);
    }
    
    @Test
    public void testStat() {
        List<SearchCriteria> searchCriterias = new ArrayList<>();
        SearchCriteria subCriteria = new SearchCriteria();
        subCriteria.setOption(new SearchOption(SearchLogic.must, SearchType.range));
        subCriteria.setField("activeTime");
        subCriteria.addFieldValue("2019-05-16T17:43:43+0800");
        subCriteria.addFieldValue("2019-06-04T10:40:25+0800");
        searchCriterias.add(subCriteria);
        StatResult sr = client.stat(searchCriterias, "activeTime");
        assertTrue(sr.getCount() == 6);
        assertTrue(sr.getMaxTxt().equalsIgnoreCase("2019-06-04T02:40:25.000Z") );
    }
}
