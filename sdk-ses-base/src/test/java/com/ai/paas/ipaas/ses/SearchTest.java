package com.ai.paas.ipaas.ses;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.search.ISearchClient;
import com.ai.paas.ipaas.search.SearchCmpClientFactory;
import com.ai.paas.ipaas.search.common.JsonBuilder;
import com.ai.paas.ipaas.search.vo.AggResult;
import com.ai.paas.ipaas.search.vo.AggField;
import com.ai.paas.ipaas.search.vo.Result;
import com.ai.paas.ipaas.search.vo.SearchCriteria;
import com.ai.paas.ipaas.search.vo.SearchOption;
import com.ai.paas.ipaas.search.vo.SearchOption.SearchLogic;
import com.ai.paas.ipaas.search.vo.SearchOption.SearchType;
import com.ai.paas.ipaas.search.vo.SearchOption.TermOperator;
import com.ai.paas.ipaas.search.vo.Sort;
import com.ai.paas.ipaas.search.vo.Sort.SortOrder;
import com.ai.paas.util.DateTimeUtil;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class SearchTest {
    private Logger logger = LoggerFactory.getLogger(SearchTest.class);
    static ISearchClient client = null;
    static String indexName = "user";
    static String mapping = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        String hosts = "localhost:9200";
        mapping = "{" + "   \"userInfo\" : {" + "     \"properties\" : {"
                + "     	\"userId\" :  {\"type\" : \"string\", \"store\" : \"yes\",\"index\": \"not_analyzed\"},"
                + "       	\"name\" : {\"type\" : \"string\", \"store\" : \"yes\",\"analyzer\":\"nGram_analyzer\"},"
                + "       	\"age\" : {\"type\" : \"integer\"},"
                + "       	\"created\" : {\"type\" : \"date\", \"format\" : \"strict_date_optional_time||epoch_millis\"}"
                + "     }" + "   }" + " }";
        String id = "userId";
        client = SearchCmpClientFactory.getSearchClient(hosts, indexName, id, "elastic", "123456");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        client = null;
    }

    @Before
    public void setUp() throws Exception {
        // if (client.existIndex(indexName))
        // client.deleteIndex(indexName);
        // client.createIndex(indexName, 3, 1);
    }

    @After
    public void tearDown() throws Exception {
        // if (client.existIndex(indexName))
        // client.deleteIndex(indexName);
    }

    @Test
    public void testInsertMapOfStringObject() {
        Map<String, Object> data = new HashMap<>();
        data.put("userId", "102");
        data.put("name", "这是一个中文测试句子，this is a text");
        data.put("age", 29);
        data.put("created", DateTimeUtil.format(new Date()));
        assertTrue(client.insert(data));
    }

    @Test
    public void testInsertString() {
        String data = "{\"userId\":103,\"name\":\"爱丢恶化缺乏\",\"age\":30,\"created\":\"2016-06-17T23:15:09\",\"test\":\"中华人民共和国\"}";
        assertTrue(client.insert(data));
    }

    @Test
    public void testInsertT() {
        User user = new User("105", "当萨菲罗斯开发送发了多少分旬", 31, new Date());
        assertTrue(client.insert(user));
    }

    @Test
    public void testInsertJsonBuilder() throws Exception {
        JsonBuilder jsonBuilder = new JsonBuilder().startObject().field("userId", 106).field("name", "每逢佳节倍思亲")
                .field("age", 31).field("created", new Date()).endObject();
        assertTrue(client.insert(jsonBuilder));
    }

    @Test
    public void testDeleteString() {
        testInsertT();
        assertTrue(client.delete("105"));
    }

    @Test
    public void testBulkDelete() {
        User user = null;
        user = new User("105", "当萨菲罗斯开发送发了多少分旬", 31, new Date());
        client.insert(user);
        user = new User("106", "当萨菲罗斯开发送发了多少分旬", 31, new Date());
        client.insert(user);
        user = new User("107", "当萨菲罗斯开发送发了多少分旬", 31, new Date());
        List<String> ids = new ArrayList<>();
        ids.add("105");
        ids.add("106");
        ids.add("107");
        assertTrue(client.bulkDelete(ids) == 3);
    }

    @Test
    public void testDeleteListOfSearchCriteria() {
        client.clean();
        client.refresh();
        User user = null;
        user = new User("105", "当萨菲罗斯开发送发了多少分旬123", 31, new Date());
        client.insert(user);
        user = new User("106", "当萨菲罗斯开发送发了多少分旬萨达", 32, new Date());
        client.insert(user);
        user = new User("107", "当萨菲罗斯开发送发了多少分旬萨达", 33, new Date());
        client.insert(user);
        client.refresh();
        List<SearchCriteria> searchCriterias = new ArrayList<>();
        SearchCriteria searchCriteria = new SearchCriteria();
        searchCriteria.setField("name");
        List<Object> values = new ArrayList<Object>();
        values.add("开发");
        searchCriteria.setFieldValue(values);
        searchCriterias.add(searchCriteria);
        assertTrue(client.delete(searchCriterias) == 3);
    }

    @Test
    public void testClean() {
        User user = null;
        user = new User("105", "当萨菲罗斯开发送发了多少分旬123", 31, new Date());
        client.insert(user);
        user = new User("106", "当萨菲罗斯开发送发了多少分旬萨达", 32, new Date());
        client.insert(user);
        user = new User("107", "当萨菲罗斯开发送发了多少分旬萨达", 33, new Date());
        client.insert(user);
        assertTrue(client.clean());
        user = new User("105", "当萨菲罗斯开发送发了多少分旬123", 31, new Date());
        assertTrue(client.insert(user));
    }

    @Test
    public void testUpdateStringMapOfStringObject() {
        User user = null;
        user = new User("105", "当萨菲罗斯开发送发了多少分旬123", 31, new Date());
        client.insert(user);
        Map<String, String> data = new HashMap<>();
        data.put("name", "不知道改变了没有");
        assertTrue(client.update("105", data));
    }

    @Test
    public void testUpdateStringString() {
        User user = null;
        user = new User("105", "当萨菲罗斯开发送发了多少分旬123", 31, new Date());
        client.insert(user);
        String json = "{\"name\":\"不知道改变了没有\"}";
        assertTrue(client.update("105", json));
    }

    @Test
    public void testUpdateStringT() {
        User user = null;
        user = new User("105", "当萨菲罗斯开发送发了多少分旬123", 31, new Date());
        client.insert(user);
        user.setName("不知道改变了没有");
        assertTrue(client.update("105", user));
    }

    @Test
    public void testUpdateStringJsonBuilder() throws Throwable, Exception {
        User user = null;
        user = new User("105", "当萨菲罗斯开发送发了多少分旬123", 31, new Date());
        client.insert(user);
        JsonBuilder jsonBuilder = new JsonBuilder().startObject().field("name", "每逢佳节倍思亲").endObject();
        assertTrue(client.update("105", jsonBuilder));
    }

    @Test
    public void testUpsertStringMapOfStringObject() {
        Map<String, Object> data = new HashMap<>();
        data.put("name", "不知道改变了没有");
        assertTrue(client.upsert("105", data));
        data.put("name", "不知道改变了没有123");
        data.put("age", 32);
        assertTrue(client.upsert("105", data));
    }

    @Test
    public void testUpsertStringString() {
        String json = "{\"name\":\"不知道改变了没有\"}";
        assertTrue(client.upsert("105", json));
        json = "{\"name\":\"不知道改变了没有\",\"age\":34}";
        assertTrue(client.upsert("105", json));
    }

    @Test
    public void testUpsertStringT() {
        User user = new User();
        user.setUserId("105");
        user.setName("不知道改变了没有");
        assertTrue(client.upsert("105", user));
    }

    @Test
    public void testUpsertStringJsonBuilder() throws Throwable, Exception {
        JsonBuilder jsonBuilder = new JsonBuilder().startObject().field("name", "每逢佳节倍思亲").endObject();
        assertTrue(client.upsert("105", jsonBuilder));
        jsonBuilder = new JsonBuilder().startObject().field("age", 56).endObject();
        assertTrue(client.upsert("105", jsonBuilder));
    }

    @Test
    public void testBulkMapInsert() {
        List<String> ids = new ArrayList<>();
        ids.add("101");
        ids.add("102");
        ids.add("103");
        client.bulkDelete(ids);
        Map<String, Object> data1 = new HashMap<>();
        data1.put("userId", "101");
        data1.put("name", "这是一个中文测试句子，this is a text1");
        data1.put("age", 30);
        data1.put("created", DateTimeUtil.format(new Date()));
        Map<String, Object> data2 = new HashMap<>();
        data2.put("userId", "102");
        data2.put("name", "这是一个中文测试句子，this is a text2");
        data2.put("age", 31);
        data2.put("created", DateTimeUtil.format(new Date()));
        Map<String, Object> data3 = new HashMap<>();
        data3.put("userId", "103");
        data3.put("name", "这是一个中文测试句子，this is a text3");
        data3.put("age", 32);
        data3.put("created", DateTimeUtil.format(new Date()));
        List<Map<String, Object>> data = new ArrayList<>();
        data.add(data1);
        data.add(data2);
        data.add(data3);
        client.bulkMapInsert(data);

    }

    @Test
    public void testBulkJsonInsert() {
        client.clean();
        List<String> datas = new ArrayList<>();
        String data1 = "{\"userId\":103,\"name\":\"当萨菲罗斯开发送发了多少分旬1234\",\"age\":30,\"created\":\"2016-06-17T23:15:09\"}";
        String data2 = "{\"userId\":104,\"name\":\"当萨菲罗斯开发送发了多少分旬1235\",\"age\":30,\"created\":\"2016-06-17T23:15:09\"}";
        String data3 = "{\"userId\":101,\"name\":\"当萨菲罗斯开发送发了多少分旬1236\",\"age\":30,\"created\":\"2016-06-17T23:15:09\"}";
        datas.add(data1);
        datas.add(data2);
        datas.add(data3);
        client.bulkJsonInsert(datas);
    }

    @Test
    public void testBulkInsertListOfT() {
        client.clean();
        List<User> datas = new ArrayList<>();
        User user1 = new User("105", "Test ABC 芦玉Match123", 31, new Date());
        User user2 = new User("106", "test 开发 芦match玉", 41, new Date());
        User user3 = new User("", "This 开发 is a 芦玉test", 31, null);
        User user4 = new User("", "This 开发 is a 芦玉test", 31, null);
        datas.add(user1);
        datas.add(user2);
        datas.add(user3);
        datas.add(user4);
        client.bulkInsert(datas);
    }

    @Test
    public void testBulkInsertSetOfJsonBuilder() throws Exception {
        client.clean();
        JsonBuilder jsonBuilder1 = new JsonBuilder().startObject().field("userId", 106).field("name", "每逢佳节倍思亲1")
                .field("age", 31).field("created", new Date()).endObject();
        JsonBuilder jsonBuilder2 = new JsonBuilder().startObject().field("userId", 107).field("name", "每逢佳节倍思亲2")
                .field("age", 31).field("created", new Date()).endObject();
        JsonBuilder jsonBuilder3 = new JsonBuilder().startObject().field("userId", 108).field("name", "每逢佳节倍思亲3")
                .field("age", 31).field("created", DateTimeUtil.parse("2017-09-18 22:10:04")).endObject();
        JsonBuilder jsonBuilder4 = new JsonBuilder().startObject().field("userId", 109).field("name", "每逢佳节倍思亲3")
                .field("age", 31).field("created", DateTimeUtil.parse("2017-09-18 22:10:04")).endObject();
        Set<JsonBuilder> datas = new HashSet<>();
        datas.add(jsonBuilder1);
        datas.add(jsonBuilder2);
        datas.add(jsonBuilder3);
        datas.add(jsonBuilder4);
        client.bulkInsert(datas);
        client.refresh();
    }

    @Test
    public void testBulkMapUpdate() {
        client.clean();
        testBulkMapInsert();
        List<String> ids = new ArrayList<>();
        Map<String, Object> data1 = new HashMap<>();
        ids.add("101");
        data1.put("name", "这是一个中文测试句子123，this is a text1");
        Map<String, Object> data2 = new HashMap<>();
        ids.add("102");
        data2.put("name", "这是一个中文测试句子1233，this is a text2");
        Map<String, Object> data3 = new HashMap<>();
        ids.add("103");
        data3.put("name", "这是一个中文测试句子123456，this is a text3");
        List<Map<String, Object>> data = new ArrayList<>();
        data.add(data1);
        data.add(data2);
        data.add(data3);
        assertTrue(client.bulkMapUpdate(ids, data) == 3);
    }

    @Test
    public void testBulkJsonUpdate() {
        client.clean();
        testBulkJsonInsert();
        List<String> ids = new ArrayList<>();
        ids.add("101");
        ids.add("103");
        ids.add("105");
        List<String> datas = new ArrayList<>();
        String data1 = "{\"name\":\"爱丢恶化缺乏11\",\"age\":31,\"created\":\"2016-06-17T23:15:09\"}";
        String data2 = "{\"name\":\"爱丢恶化缺乏22\",\"age\":32,\"created\":\"2016-06-17T23:15:09\"}";
        String data3 = "{\"name\":\"爱丢恶化缺乏33\",\"age\":33,\"created\":\"2016-06-17T23:15:09\"}";
        datas.add(data1);
        datas.add(data2);
        datas.add(data3);
        assertTrue(client.bulkJsonUpdate(ids, datas) == 3);
    }

    @Test
    public void testBulkUpdateListOfStringListOfT() {
        client.clean();
        testBulkInsertListOfT();
        List<User> datas = new ArrayList<>();
        User user1 = new User();
        User user2 = new User();
        User user3 = new User();
        user1.setName("三发松岛枫1");
        user2.setName("三发松岛枫2");
        user3.setName("三发松岛枫3");
        datas.add(user1);
        datas.add(user2);
        datas.add(user3);
        List<String> ids = new ArrayList<>();
        ids.add("105");
        ids.add("106");
        ids.add("107");
        assertTrue(client.bulkUpdate(ids, datas) == 3);
    }

    @Test
    public void testBulkUpdateListOfStringSetOfJsonBuilder() throws Throwable {
        client.clean();
        testBulkInsertSetOfJsonBuilder();
        JsonBuilder jsonBuilder1 = new JsonBuilder().startObject().field("name", "每逢佳节倍思亲1").endObject();
        JsonBuilder jsonBuilder2 = new JsonBuilder().startObject().field("name", "每逢佳节倍思亲2").endObject();
        JsonBuilder jsonBuilder3 = new JsonBuilder().startObject().field("name", "每逢佳节倍思亲3").endObject();
        Set<JsonBuilder> datas = new HashSet<>();
        datas.add(jsonBuilder1);
        datas.add(jsonBuilder2);
        datas.add(jsonBuilder3);
        List<String> ids = new ArrayList<>();
        ids.add("105");
        ids.add("106");
        ids.add("107");
        assertTrue(client.bulkUpdate(ids, datas) == 3);
    }

    @Test
    public void testSearchBySQL() {
        testBulkInsertListOfT();
        client.refresh();
        String qry = "age:(>=10 AND <55)";
        Result<User> result = client.searchBySQL(qry, 0, 10, null, User.class);
        assertTrue(result.getCount() == 4);
        qry = "age:(>=10 AND <55) AND name:ABC";
        result = client.searchBySQL(qry, 0, 10, null, User.class);
        assertTrue(result.getCount() == 1);
        qry = "age:(>=40 AND <55) ";
        result = client.searchBySQL(qry, 0, 1, null, User.class);
        assertTrue(result.getCount() == 1);
    }

    @Test
    public void testSearchMatch() {
        client.clean();
        testBulkInsertListOfT();
        client.refresh();
        List<SearchCriteria> searchCriterias = new ArrayList<>();
        SearchCriteria searchCriteria = new SearchCriteria("name", "match",
                new SearchOption(SearchOption.SearchLogic.should, SearchOption.SearchType.match, TermOperator.AND));
        searchCriterias.add(searchCriteria);
        Result<User> result = client.search(searchCriterias, 0, 10, null, User.class);
        logger.info("{}", result.getCount());
        assertTrue(result.getCount() == 1);
    }

    @Test
    public void testSearchRange() {
        client.clean();
        testBulkInsertListOfT();
        client.refresh();
        List<SearchCriteria> searchCriterias = new ArrayList<>();
        SearchCriteria searchCriteria = new SearchCriteria("age",
                new SearchOption(SearchOption.SearchLogic.must, SearchOption.SearchType.range));
        List<Object> values = new ArrayList<>();
        values.add(41);
        searchCriteria.setFieldValue(values);
        searchCriterias.add(searchCriteria);
        Result<User> result = client.search(searchCriterias, 0, 10, null, User.class);
        assertTrue(result.getCount() == 1);
    }

    @Test
    public void testSearch() {
        client.clean();
        testBulkInsertListOfT();
        client.refresh();
        List<SearchCriteria> searchCriterias = new ArrayList<>();
        SearchCriteria searchCriteria = new SearchCriteria("age", "31", new SearchOption());
        searchCriterias.add(searchCriteria);
        Result<User> result = client.search(searchCriterias, 0, 10, null, User.class);
        logger.info("{}", result.getCount());
        assertTrue(result.getCount() == 3);
        // 复杂查询，name含有“开发”或者含有“1234”且年龄在31-45之间的
        searchCriteria = new SearchCriteria();
        SearchCriteria subCriteria = new SearchCriteria("age", "41",
                new SearchOption(SearchLogic.must_not, SearchType.querystring));
        searchCriteria.addSubCriteria(subCriteria);
        SearchCriteria subCriteria1 = new SearchCriteria("name", "开发",
                new SearchOption(SearchLogic.should, SearchType.querystring));
        searchCriteria.addSubCriteria(subCriteria1);
        searchCriterias.clear();
        searchCriterias.add(searchCriteria);
        SearchCriteria searchCriteria1 = new SearchCriteria();
        searchCriteria1.setOption(new SearchOption(SearchLogic.must, SearchType.range));
        searchCriteria1.setField("age");
        searchCriteria1.addFieldValue("31");
        searchCriteria1.addFieldValue("45");
        searchCriterias.add(searchCriteria1);
        result = client.search(searchCriterias, 0, 10, null, User.class);
        logger.info("{}", result.getCount());
        assertTrue(result.getCount() == 2);
    }

    @Test
    public void testSearchByDSL() {
        client.clean();
        testBulkInsertListOfT();
        client.refresh();
        String qry = " { " + "\"bool\": {" + "\"must\": [" + "  { \"match\": { \"name\":   \"开发\"}}" + "]" + "}" + "}";
        Result<User> result = client.searchByDSL(qry, 0, 10, null, User.class);
        assertTrue(result.getCount() == 3);
        String data = "{\"userId\":103,\"name\":\"爱丢恶化缺乏\",\"age\":30,\"created\":\"2016-06-17T23:15:09\",\"test\":\"中华人民共和国\"}";
        client.insert(data);
        client.refresh();
        qry = " { " + "\"bool\": {" + "\"must\": [" + "  { \"match\": { \"test\":   \"中华\"}}" + "]" + "}" + "}";
        result = client.searchByDSL(qry, 0, 10, null, User.class);
        logger.info("{}", result.getCount());
        assertTrue(result.getCount() == 1);
    }

    @Test
    public void testSearchByDSLString() {
        testBulkInsertListOfT();
        client.refresh();
        String qry = " { " + "\"bool\": {" + "\"must\": [" + "  { \"match\": { \"name\":   \"开发\"}},"
                + "        { \"match\": { \"age\": 41 }}" + "]," + "\"filter\": ["
                + "{ \"term\":  { \"userId\": \"106\" }}," + "{ \"range\": { \"created\": { \"gte\": \"2016-06-20\" }}}"
                + "]" + "}" + "}";
        logger.info("{}", qry);
        String result = client.searchByDSL(qry, 0, 10, null);
        Gson gson = new Gson();
        JsonObject json = gson.fromJson(result, JsonObject.class);
        assertTrue(json.get("count").getAsInt() == 1);
    }

    @Test
    public void getSuggestStringString() {
        String mapping = "{    \"properties\" : {"
                + "         \"userId\" :  {\"type\" : \"text\", \"store\" : \"true\",\"index\": \"false\"},"
                + "         \"name\" : {\"type\" : \"completion\", \"analyzer\":\"ik_max_word\"},"
                + "         \"age\" : {\"type\" : \"integer\"},"
                + "         \"created\" : {\"type\" : \"date\", \"format\" : \"strict_date_optional_time||epoch_millis\"}"
                + "     }" + " }";
        if (client.existIndex("user"))
            client.deleteIndex("user");
        client.createIndex("user", 1, 1);
        logger.info("{}", mapping);
        client.addMapping("user", mapping);
        client.clean();
        testBulkInsertListOfT();
        client.refresh();
        List<String> suggests = client.getSuggest("name", "开", 10);
        logger.info("{}", suggests.size());
        assertTrue(suggests.size() == 2);
        suggests = client.getSuggest("name", "4", 10);
        // 因为搜索所有的字段，只要含有就显示
        logger.info("{}", suggests);
        assertTrue(suggests.size() == 0);
    }

    @Test
    public void testAggregateListOfSearchCriteriaString() {
        client.clean();
        testBulkInsertListOfT();
        client.refresh();
        List<SearchCriteria> searchCriterias = new ArrayList<>();
        SearchCriteria searchCriteria = new SearchCriteria();
        searchCriteria.setField("age");
        List<Object> values = new ArrayList<Object>();
        values.add("41");
        searchCriteria.setFieldValue(values);
        searchCriterias.add(searchCriteria);
        Result<Map<String, Long>> result = client.aggregate(searchCriterias, "age");
        assertTrue(result.getCount() == 1);
    }

    @Test
    public void testAggregateListOfSearchCriteriaListOfString() {
        String mapping = "{    \"properties\" : {"
                + "         \"userId\" :  {\"type\" : \"text\", \"store\" : \"true\",\"index\": \"false\"},"
                + "         \"name\" : {\"type\" : \"text\", \"analyzer\":\"ik_max_word\"},"
                + "         \"age\" : {\"type\" : \"integer\"},"
                + "         \"created\" : {\"type\" : \"date\", \"format\" : \"strict_date_optional_time||epoch_millis\"}"
                + "     }" + " }";
        if (client.existIndex("user"))
            client.deleteIndex("user");
        client.createIndex("user", 1, 1);
        logger.info("{}", mapping);
        client.addMapping("user", mapping);
        client.clean();
        testBulkInsertListOfT();
        testBulkJsonInsert();
        client.refresh();
        List<SearchCriteria> searchCriterias = new ArrayList<>();
        SearchCriteria searchCriteria = new SearchCriteria();
        searchCriteria.setField("name");
        List<Object> values = new ArrayList<Object>();
        values.add("开发");
        searchCriteria.setFieldValue(values);
        searchCriterias.add(searchCriteria);
        List<AggField> fields = new ArrayList<>();
        fields.add(new AggField("age"));
        Result<List<AggResult>> result = client.aggregate(searchCriterias, fields);
        assertTrue(result.getAggs().size() == 3);
    }

    @Test
    public void testFullTextSearchWithFieldWithAgg() {
        testBulkInsertListOfT();
        testBulkJsonInsert();
        testBulkInsertListOfT();
        testBulkInsertListOfT();
        testBulkInsertListOfT();
        testBulkInsertListOfT();
        testBulkInsertListOfT();
        testBulkInsertListOfT();
        client.refresh();
        String text = "开发";
        List<AggField> fields = new ArrayList<>();
        fields.add(new AggField("age"));
        List<String> qryFields = new ArrayList<>();
        qryFields.add("name");
        List<Sort> sorts = new ArrayList<>();
        Sort sort = new Sort("age", SortOrder.ASC);
        sorts.add(sort);
        Result<User> result = client.fullTextSearch("name", text, fields, 0, 10, sorts, User.class);
        assertTrue(result.getCount() == 18);
    }

    @Test
    public void testFullTextSearchWithAgg() {
        client.clean();
        testBulkInsertListOfT();
        testBulkJsonInsert();
        client.refresh();
        String text = "开发";
        List<AggField> fields = new ArrayList<>();
        fields.add(new AggField("age"));
        List<String> qryFields = new ArrayList<>();
        qryFields.add("name");
        Result<User> result = client.fullTextSearch(text, qryFields, fields, 0, 10, null, User.class);
        assertTrue(result.getCount() == 6);
    }

    @Test
    public void testFullTextSearch() {
        client.clean();
        testBulkInsertListOfT();
        client.refresh();
        String text = "开发";

        Result<User> result = client.fullTextSearch("name", text, 0, 10, null, User.class);
        assertTrue(result.getCount() == 3);
    }

    @Test
    public void testGetByIdStringClassOfT() {
        client.clean();
        testBulkInsertListOfT();
        client.refresh();
        User user = client.getById("105", User.class);
        assertTrue(user.getAge() == 31);
    }

    @Test
    public void testGetByIdString() {
        client.clean();
        testBulkInsertListOfT();
        client.refresh();
        String user = client.getById("105");
        Gson gson = new Gson();
        assertTrue(gson.fromJson(user, JsonObject.class).get("age").getAsInt() == 31);
    }

    @Test
    public void testCreateIndex() {
        // 先删除
        if (client.existIndex("user"))
            client.deleteIndex(indexName);
        assertTrue(client.createIndex("user", 1, 1));
    }

    @Test
    public void testDeleteIndex() {
        assertTrue(client.deleteIndex("user"));
    }

    @Test
    public void testExistIndex() {
        testCreateIndex();
        assertTrue(client.existIndex(indexName));
    }

    @Test
    public void testAddMapping() {

        if (client.existMapping("user", "user"))
            client.deleteIndex("user");
        testCreateIndex();
        String mapping2 = "{" + "     	\"userId\" :  {\"type\" : \"text\", \"store\" : \"true\",\"index\": \"false\"},"
                + "       	\"name\" : {\"type\" : \"text\", \"store\" : \"true\"},"
                + "       	\"age\" : {\"type\" : \"integer\"},"
                + "       	\"created\" : {\"type\" : \"date\", \"format\" : \"strict_date_optional_time||epoch_millis\"}"
                + " }";

        assertTrue(client.addMapping("user", mapping2, false));
    }

    @Test
    public void testExistingMapping() {
        assertTrue(client.existMapping("user", "user"));
    }

}
