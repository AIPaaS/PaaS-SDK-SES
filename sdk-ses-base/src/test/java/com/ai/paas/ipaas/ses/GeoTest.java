package com.ai.paas.ipaas.ses;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.search.ISearchClient;
import com.ai.paas.ipaas.search.SearchCmpClientFactory;
import com.ai.paas.ipaas.search.vo.GeoLocation;
import com.ai.paas.ipaas.search.vo.Result;
import com.ai.paas.ipaas.search.vo.SearchCriteria;
import com.ai.paas.ipaas.search.vo.SearchOption;
import com.ai.paas.ipaas.search.vo.SearchOption.TermOperator;
import com.ai.paas.util.JsonUtil;

public class GeoTest {
    static ISearchClient client = null;
    static String indexName = "geotest";
    private Logger logger = LoggerFactory.getLogger(GeoTest.class);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String hosts = "localhost:9200";
        String id = "_id";
        client = SearchCmpClientFactory.getSearchClient(hosts, indexName, id, "elastic", "123456");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        client.close();
        client = null;
    }

    public void setUp() {
        client.deleteIndex(indexName);
        if (!client.existIndex(indexName)) {
            client.createIndex(indexName, 1, 1);
            // 增加mapping
            String mapping = "{\"properties\": {\n" + "               \"marketName\":{\n"
                    + "                    \"type\": \"text\"\n" + "                },\n"
                    + "              \"location\": {\n" + "                \"type\": \"geo_point\"\n"
                    + "              }}}";
            client.addMapping(indexName, mapping);
        }
        client.clean();
        List<String> datas = new ArrayList<>();
        String data1 = "{\"marketName\":\"吴中商场\",\"location\": { \n" + "        \"lat\": 31.12,\n"
                + "        \"lon\": -51.34\n" + "      }}";
        String data2 = "{\"marketName\":\"真北商场\",\"location\": { \n" + "        \"lat\": 41.12,\n"
                + "        \"lon\": -71.34\n" + "      }}";
        String data3 = "{\"marketName\":\"西安商场\",\"location\": { \n" + "        \"lat\": 51.12,\n"
                + "        \"lon\": 31.34\n" + "      }}";
        String data4 = "{\"marketName\":\"吴中大厦\",\"location\": { \n" + "        \"lat\": 31.12,\n"
                + "        \"lon\": -51.34\n" + "      }}";
        datas.add(data1);
        datas.add(data2);
        datas.add(data3);
        datas.add(data4);
        client.bulkJsonInsert(datas);
        client.refresh();
    }

    @Test
    public void testGeoDistance() {
        setUp();
        GeoLocation geoWhere = new GeoLocation("location", 31.119999, -51.34, 200);
        QueryBuilder queryBuilder = QueryBuilders.matchQuery("marketName", "商场");
        Result<GEOTestVO> r = client.geoDistanceQuery(queryBuilder, 0, 10, GEOTestVO.class, geoWhere);
        assertTrue(r.getCount() == 1);
        assertTrue(r.getContents().get(0).getDistance() >= 0);
        logger.info("{}", JsonUtil.toJson(r));
    }

    @Test
    public void testGeoDistanceListWhere() {
        setUp();
        GeoLocation geoFilter = new GeoLocation("location", 31.119999, -51.34, 200);
        List<SearchCriteria> searchCriterias = new ArrayList<>();
        SearchCriteria where = new SearchCriteria("marketName", "商场",
                new SearchOption(SearchOption.SearchLogic.should, SearchOption.SearchType.match, TermOperator.AND));
        searchCriterias.add(where);
        Result<GEOTestVO> r = client.geoDistanceQuery(searchCriterias, 0, 10, GEOTestVO.class, geoFilter);
        assertTrue(r.getCount() == 1);
        assertTrue(r.getContents().get(0).getDistance() >= 0);
        logger.info("{}", JsonUtil.toJson(r));
    }

}
