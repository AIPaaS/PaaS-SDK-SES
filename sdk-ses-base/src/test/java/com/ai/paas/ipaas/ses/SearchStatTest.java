package com.ai.paas.ipaas.ses;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.search.ISearchClient;
import com.ai.paas.ipaas.search.SearchCmpClientFactory;
import com.ai.paas.ipaas.search.vo.SearchCriteria;
import com.ai.paas.ipaas.search.vo.SearchOption;
import com.ai.paas.ipaas.search.vo.SearchOption.SearchLogic;
import com.ai.paas.ipaas.search.vo.SearchOption.SearchType;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.ai.paas.ipaas.search.vo.StatResult;

public class SearchStatTest {
    static ISearchClient client = null;
    static String indexName = "user";
    private Logger logger = LoggerFactory.getLogger(SearchStatTest.class);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String hosts = "localhost:9200";
        String id = "_id";
        client = SearchCmpClientFactory.getSearchClient(hosts, indexName, id, "elastic", "123456");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        client = null;
    }

    public void setUp() {
        client.clean();
        List<String> datas = new ArrayList<>();
        String data1 = "{\"userId\":103,\"name\":\"当萨菲罗斯开发送发了多少分旬1234\",\"age\":30,\"created\":\"2016-06-17T23:15:09+0800\"}";
        String data2 = "{\"userId\":104,\"name\":\"当萨菲罗斯开发送发了多少分旬1235\",\"age\":31,\"created\":\"2016-06-18T23:15:09+0800\"}";
        String data3 = "{\"userId\":101,\"name\":\"当萨菲罗斯开发送发了多少分旬1236\",\"age\":41,\"created\":\"2016-06-19T23:15:09+0800\"}";
        datas.add(data1);
        datas.add(data2);
        datas.add(data3);
        client.bulkJsonInsert(datas);
        client.refresh();
    }

    @Test
    public void testCount() {
        setUp();
        List<SearchCriteria> searchCriterias = new ArrayList<>();
        SearchCriteria subCriteria = new SearchCriteria();
        subCriteria.setOption(new SearchOption(SearchLogic.must, SearchType.range));
        subCriteria.setField("created");
        subCriteria.addFieldValue("2016-06-18T23:15:09");
        subCriteria.addFieldValue("2016-06-19T23:15:09");
        searchCriterias.add(subCriteria);
        StatResult sr = client.count(searchCriterias, "age");
        assertTrue(sr.getCount() == 2);
    }

    @Test
    public void testStat() {
        setUp();
        List<SearchCriteria> searchCriterias = new ArrayList<>();
        SearchCriteria subCriteria = new SearchCriteria();
        subCriteria.setOption(new SearchOption(SearchLogic.must, SearchType.range));
        subCriteria.setField("created");
        subCriteria.addFieldValue("2016-06-18T23:15:09");
        subCriteria.addFieldValue("2016-06-19T23:15:09");
        searchCriterias.add(subCriteria);
        StatResult sr = client.stat(searchCriterias, "age");
        assertTrue(sr.getCount() == 2);

        logger.info("{}", sr.getMaxTxt());
        assertTrue(sr.getMax() == 41.0);
    }

    @Test
    public void testCountGroupBy() {
        setUp();
        List<SearchCriteria> searchCriterias = new ArrayList<>();
        SearchCriteria subCriteria = new SearchCriteria();
        subCriteria.setOption(new SearchOption(SearchLogic.must, SearchType.range));
        subCriteria.setField("created");
        subCriteria.addFieldValue("2016-06-18T23:15:09");
        subCriteria.addFieldValue("2016-06-19T23:15:09");
        searchCriterias.add(subCriteria);
        List<StatResult> results = client.count(searchCriterias, "age", "age");
        Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create();
        logger.info("{}", gson.toJson(results));
        assertTrue(results.size() == 2);
    }

    @Test
    public void testStatGroupBy() {
        setUp();
        List<SearchCriteria> searchCriterias = new ArrayList<>();
        SearchCriteria subCriteria = new SearchCriteria();
        subCriteria.setOption(new SearchOption(SearchLogic.must, SearchType.range));
        subCriteria.setField("created");
        subCriteria.addFieldValue("2016-06-18T23:15:09");
        subCriteria.addFieldValue("2016-06-19T23:15:09");
        searchCriterias.add(subCriteria);
        List<StatResult> results = client.stat(searchCriterias, "age", "age");
        Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create();
        logger.info("{}", gson.toJson(results));
        assertTrue(results.size() == 2);
    }
}
