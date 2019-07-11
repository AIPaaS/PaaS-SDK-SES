package com.ai.paas.ipaas.search.impl;

//搜索实现定义

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder.Operator;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.aggregations.metrics.stats.StatsBuilder;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCountBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.PaaSConstant;
import com.ai.paas.ipaas.search.ISearchClient;
import com.ai.paas.ipaas.search.SearchRuntimeException;
import com.ai.paas.ipaas.search.common.DynamicMatchOption;
import com.ai.paas.ipaas.search.common.JsonBuilder;
import com.ai.paas.ipaas.search.common.TypeGetter;
import com.ai.paas.ipaas.search.vo.AggField;
import com.ai.paas.ipaas.search.vo.AggResult;
import com.ai.paas.ipaas.search.vo.Result;
import com.ai.paas.ipaas.search.vo.SearchCriteria;
import com.ai.paas.ipaas.search.vo.Sort;
import com.ai.paas.ipaas.search.vo.StatResult;
import com.ai.paas.ipaas.util.Assert;
import com.ai.paas.ipaas.util.StringUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class SearchClientImpl implements ISearchClient {
    private Logger logger = LoggerFactory.getLogger(SearchClientImpl.class);
    private String highlightCSS = "span,span";
    private String indexName;
    private String id = null;
    private static Settings settings = Settings.settingsBuilder().put("client.transport.ping_timeout", "60s")
            .put("client.transport.sniff", "true").put("client.transport.ignore_cluster_name", "true").build();
    private String hosts = null;
    // 创建私有对象
    private TransportClient client;

    private String esDateFmt = "yyyy-MM-dd'T'HH:mm:ssZZZ";
    private Gson esgson = new GsonBuilder().setDateFormat(esDateFmt).create();
    private Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    private static final int BATCH_SIZE = 1000;
    private SearchHelper searchHelper = new SearchHelper();

    public SearchClientImpl(String hosts, String indexName, String id) {
        this.indexName = indexName;
        this.id = id;
        this.hosts = hosts;
        initClient();
        searchHelper.setDateFmt(this.esDateFmt);
    }

    @Override
    public void setESDateFmt(String esDateFmt, String dateFmt) {
        this.esDateFmt = esDateFmt;
        esgson = new GsonBuilder().setDateFormat(this.esDateFmt).create();
        searchHelper.setDateFmt(this.esDateFmt);
        gson = new GsonBuilder().setDateFormat(dateFmt).create();
    }

    public void initClient() {
        // 如果客户端不为空，且还连接到某个节点则复用
        if (null != client && !client.connectedNodes().isEmpty())
            return;
        List<String> clusterList = new ArrayList<>();
        try {
            client = TransportClient.builder().settings(settings).build();
            if (!StringUtil.isBlank(hosts)) {
                clusterList = Arrays.asList(hosts.split(","));
            }
            for (String item : clusterList) {
                String address = item.split(":")[0];
                int port = Integer.parseInt(item.split(":")[1]);
                /* 通过tcp连接搜索服务器，如果连接不上，有一种可能是服务器端与客户端的jar包版本不匹配 */
                client.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress(address, port)));
            }
        } catch (Exception e) {
            throw new SearchRuntimeException("ES init client error", e);
        }

    }

    // 取得实例
    public synchronized TransportClient getTransportClient() {
        return client;
    }

    public void setHighlightCSS(String highlightCSS) {
        this.highlightCSS = highlightCSS;
    }

    public List<String> getSuggest(String value, int count) {
        return getSuggest("_all", value, count);
    }

    public List<String> getSuggest(String field, String value, int count) {
        List<String> suggests = new ArrayList<>();
        if (StringUtil.isBlank(field) || StringUtil.isBlank(value) || count <= 0)
            return suggests;
        SearchResponse response = client.prepareSearch(indexName)
                .setQuery(QueryBuilders.matchQuery(field, value).operator(Operator.AND)).setFrom(0).setSize(count)
                .get();
        if (null == response)
            return suggests;
        SearchHits hits = response.getHits();
        if (hits.getTotalHits() == 0) {
            return suggests;
        }

        for (SearchHit searchHit : hits.getHits()) {
            suggests.add(searchHit.getSourceAsString());
        }
        return suggests;
    }

    @Override
    public boolean insert(Map<String, Object> data) {
        if (null == data || data.size() <= 0)
            return false;
        return insert(esgson.toJson(data));
    }

    @Override
    public boolean insert(String json) {
        if (StringUtil.isBlank(json))
            return false;
        IndexResponse response = null;
        // 判断一下是否有id字段
        String parsedId = searchHelper.getId(json, this.id);
        if (StringUtil.isBlank(parsedId)) {
            response = client.prepareIndex(indexName, indexName).setOpType(IndexRequest.OpType.CREATE).setSource(json)
                    .get();
        } else {
            response = client.prepareIndex(indexName, indexName, parsedId).setOpType(IndexRequest.OpType.CREATE)
                    .setSource(json).setRefresh(true).get();
        }
        if (null != response && response.isCreated()) {
            return true;
        } else {
            throw new SearchRuntimeException("index error!" + json, gson.toJson(response));
        }
    }

    @Override
    public <T> boolean insert(T data) {
        if (null == data)
            return false;
        return insert(esgson.toJson(data));
    }

    @Override
    public boolean insert(JsonBuilder jsonBuilder) {
        if (null == jsonBuilder)
            return false;
        // 判断是否有id
        XContentBuilder builder = null;
        try {
            builder = jsonBuilder.getBuilder();
            searchHelper.hasId(builder, id);
            IndexResponse response = client.prepareIndex(indexName, indexName).setSource(builder).setRefresh(true)
                    .get();
            if (null != response && response.isCreated()) {
                return true;
            } else {
                throw new SearchRuntimeException("index error!", jsonBuilder.getBuilder().toString());
            }
        } catch (Exception e) {
            throw new SearchRuntimeException(jsonBuilder.toString(), e);
        } finally {
            if (null != builder)
                builder.close();
        }
    }

    @Override
    public boolean delete(String id) {
        if (StringUtil.isBlank(id))
            throw new SearchRuntimeException("Illegel argument,id=" + id);
        DeleteResponse response = client.prepareDelete(indexName, indexName, id).setRefresh(true).get();
        return null != response && response.isFound();
    }

    @Override
    public boolean bulkDelete(List<String> ids) {
        return bulkDelete(ids, true);
    }

    @Override
    public boolean bulkDelete(List<String> ids, boolean rebuildIndex) {
        if (null == ids || ids.isEmpty())
            return false;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        for (String localId : ids) {
            bulkRequest.add(client.prepareDelete(indexName, indexName, localId));
        }
        BulkResponse bulkResponse = bulkRequest.setRefresh(rebuildIndex).get();
        if (!bulkResponse.hasFailures()) {
            return true;
        } else {
            // 这里要做个日志，哪些成功了
            for (BulkItemResponse response : bulkResponse.getItems()) {
                logger.error("Doc id:{} is falided:{}", response.getId(), response.isFailed());
            }
            return false;
        }
    }

    @Override
    public boolean delete(List<SearchCriteria> searchCriteria) {
        return delete(searchCriteria, true);
    }

    @Override
    public boolean delete(List<SearchCriteria> searchCriteria, boolean rebuidIndex) {
        if (null == searchCriteria || searchCriteria.isEmpty())
            return false;
        // 此处要先scan出来，然后再批量删除
        List<String> ids = new ArrayList<>();
        QueryBuilder queryBuilder = null;
        queryBuilder = searchHelper.createQueryBuilder(searchCriteria);
        SearchResponse scrollResp = client.prepareSearch(indexName).setSearchType(SearchType.QUERY_THEN_FETCH)
                .setScroll(new TimeValue(60000)).setQuery(queryBuilder).setSize(100).execute().actionGet();
        while (true) {
            // 循环获取所有ids
            for (SearchHit hit : scrollResp.getHits()) {
                ids.add(hit.getId());
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute()
                    .actionGet();
            // Break condition: No hits are returned
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
        return bulkDelete(ids, rebuidIndex);
    }

    @Override
    public boolean clean() {
        // 全部删除，只能清除index，然后创建？
        // 先取出type定义
        GetMappingsResponse mappings;
        GetSettingsResponse localSettings;
        try {
            localSettings = client.admin().indices().getSettings(new GetSettingsRequest().indices(indexName)).get();
            mappings = client.admin().indices().getMappings(new GetMappingsRequest().indices(indexName)).get();
            DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest(indexName)).get();
            if (!delete.isAcknowledged()) {
                logger.error("Index wasn't deleted");
                return false;
            } else {
                // 看看是否包含这个type
                if (mappings.getMappings().containsKey(indexName)) {

                    CreateIndexResponse indexResponse = client.admin().indices().prepareCreate(indexName)
                            .setSettings(localSettings.getIndexToSettings().get(indexName)).get();

                    if (indexResponse.isAcknowledged()) {
                        PutMappingResponse putMappingResponse = client.admin().indices().preparePutMapping()
                                .setIndices(indexName).setType(indexName)
                                .setSource(mappings.getMappings().get(indexName).get(indexName).source().string())
                                .get();
                        return putMappingResponse.isAcknowledged();
                    } else
                        return false;
                } else
                    return false;
            }
        } catch (Exception e) {
            throw new SearchRuntimeException("", e);
        }
    }

    @Override
    public boolean update(String id, Map<String, Object> data) {
        if (StringUtil.isBlank(id) || null == data || data.size() <= 0)
            return false;
        UpdateResponse response = client.prepareUpdate(indexName, indexName, id).setRefresh(true)
                .setConsistencyLevel(WriteConsistencyLevel.DEFAULT).setDoc(data).setRefresh(true).get();
        return !StringUtil.isBlank(response.getId());
    }

    @Override
    public boolean update(String id, String json) {
        if (StringUtil.isBlank(id) || StringUtil.isBlank(json))
            return false;
        UpdateResponse response = client.prepareUpdate(indexName, indexName, id).setRefresh(true)
                .setConsistencyLevel(WriteConsistencyLevel.DEFAULT).setDoc(json).setRefresh(true).get();
        return !StringUtil.isBlank(response.getId());
    }

    @Override
    public <T> boolean update(String id, T data) {
        if (StringUtil.isBlank(id) || null == data)
            return false;
        return update(id, esgson.toJson(data));
    }

    @Override
    public boolean update(String id, JsonBuilder jsonBuilder) {
        if (StringUtil.isBlank(id) || null == jsonBuilder)
            return false;
        UpdateResponse response = client.prepareUpdate(indexName, indexName, id).setRefresh(true)
                .setConsistencyLevel(WriteConsistencyLevel.DEFAULT).setDoc(jsonBuilder.getBuilder()).setRefresh(true)
                .get();
        return !StringUtil.isBlank(response.getId());
    }

    @Override
    public boolean upsert(String id, Map<String, Object> data) {
        if (StringUtil.isBlank(id) || null == data || data.size() <= 0)
            return false;
        UpdateResponse response = client.prepareUpdate(indexName, indexName, id).setRefresh(true)
                .setConsistencyLevel(WriteConsistencyLevel.DEFAULT).setUpsert(data).setDoc(data).setRefresh(true).get();
        return !StringUtil.isBlank(response.getId());
    }

    @Override
    public boolean upsert(String id, String json) {
        if (StringUtil.isBlank(id) || StringUtil.isBlank(json))
            return false;
        UpdateResponse response = client.prepareUpdate(indexName, indexName, id).setRefresh(true)
                .setConsistencyLevel(WriteConsistencyLevel.DEFAULT).setUpsert(json).setDoc(json).setRefresh(true).get();
        return !StringUtil.isBlank(response.getId());
    }

    @Override
    public <T> boolean upsert(String id, T data) {
        if (StringUtil.isBlank(id) || null == data)
            return false;
        return upsert(id, esgson.toJson(data));
    }

    @Override
    public boolean upsert(String id, JsonBuilder jsonBuilder) {
        if (StringUtil.isBlank(id) || null == jsonBuilder)
            return false;
        UpdateResponse response = client.prepareUpdate(indexName, indexName, id).setRefresh(true)
                .setConsistencyLevel(WriteConsistencyLevel.DEFAULT).setUpsert(jsonBuilder.getBuilder())
                .setDoc(jsonBuilder.getBuilder()).setRefresh(true).get();
        return !StringUtil.isBlank(response.getId());
    }

    private void logBulkRespone(BulkResponse bulkResponse) {
        for (BulkItemResponse response : bulkResponse.getItems()) {
            logger.error("insert error," + response.getId() + " hasFailure=" + response.isFailed() + ", failure msg:"
                    + response.getFailureMessage());
        }
    }

    @Override
    public boolean bulkMapInsert(List<Map<String, Object>> datas) {
        return bulkMapInsert(datas, true);
    }

    @Override
    public boolean bulkMapInsert(List<Map<String, Object>> datas, boolean rebuidIndex) {
        if (null == datas || datas.isEmpty())
            return false;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        BulkResponse bulkResponse = null;
        for (Map<String, Object> data : datas) {
            if (null != data.get(id)) {
                bulkRequest.add(client.prepareIndex(indexName, indexName, data.get(id).toString()).setSource(data));
            } else {
                bulkRequest.add(client.prepareIndex(indexName, indexName).setSource(data));
            }
            if (bulkRequest.numberOfActions() > BATCH_SIZE) {
                bulkResponse = bulkRequest.setRefresh(rebuidIndex).get();
                logger.debug("batch add documents: indexed {}, hasFailures: {}", bulkRequest.numberOfActions(),
                        bulkResponse.hasFailures());
            }
        }
        if (bulkRequest.numberOfActions() > 0) {
            bulkResponse = bulkRequest.setRefresh(rebuidIndex).get();
        }
        if (null != bulkResponse && !bulkResponse.hasFailures()) {
            return true;
        } else {
            throw new SearchRuntimeException("bulk insert documents error", gson.toJson(bulkResponse));
        }
    }

    @Override
    public boolean bulkJsonInsert(List<String> jsons) {
        return bulkJsonInsert(jsons, true);
    }

    @Override
    public boolean bulkJsonInsert(List<String> jsons, boolean rebuidIndex) {
        if (null == jsons || jsons.isEmpty())
            return false;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        BulkResponse bulkResponse = null;
        for (String json : jsons) {
            if (searchHelper.hasId(json, id)) {
                bulkRequest
                        .add(client.prepareIndex(indexName, indexName, searchHelper.getId(json, id)).setSource(json));
            } else {
                bulkRequest.add(client.prepareIndex(indexName, indexName).setSource(json));
            }
            if (bulkRequest.numberOfActions() > BATCH_SIZE) {
                bulkResponse = bulkRequest.setRefresh(rebuidIndex).get();
                logger.debug("add documents: indexed {}, hasFailures: {}", bulkRequest.numberOfActions(),
                        bulkResponse.hasFailures());
            }
        }
        if (bulkRequest.numberOfActions() > 0) {
            bulkResponse = bulkRequest.setRefresh(rebuidIndex).get();
        }
        if (null != bulkResponse && !bulkResponse.hasFailures()) {
            return true;
        } else {
            throw new SearchRuntimeException("insert documents error", gson.toJson(bulkResponse));
        }
    }

    @Override
    public <T> boolean bulkInsert(List<T> datas) {
        return bulkInsert(datas, true);
    }

    @Override
    public <T> boolean bulkInsert(List<T> datas, boolean rebuidIndex) {
        if (null == datas || datas.isEmpty())
            return false;
        List<String> jsons = new ArrayList<>();
        for (T t : datas) {
            jsons.add(esgson.toJson(t));
        }
        return bulkJsonInsert(jsons, rebuidIndex);
    }

    @Override
    public boolean bulkInsert(Set<JsonBuilder> jsonBuilders) {
        return bulkInsert(jsonBuilders, true);
    }

    @Override
    public boolean bulkInsert(Set<JsonBuilder> jsonBuilders, boolean rebuidIndex) {
        if (null == jsonBuilders || jsonBuilders.isEmpty())
            return false;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        BulkResponse bulkResponse = null;
        for (JsonBuilder jsonBuilder : jsonBuilders) {
            if (searchHelper.hasId(jsonBuilder.getBuilder(), id)) {
                bulkRequest
                        .add(client.prepareIndex(indexName, indexName, searchHelper.getId(jsonBuilder.getBuilder(), id))
                                .setSource(jsonBuilder.getBuilder()));
            } else {
                bulkRequest.add(client.prepareIndex(indexName, indexName).setSource(jsonBuilder.getBuilder()));
            }
            if (bulkRequest.numberOfActions() > BATCH_SIZE) {
                bulkResponse = bulkRequest.setRefresh(rebuidIndex).get();
                logger.debug("add documents: indexed {}, hasFailures: {}", bulkRequest.numberOfActions(),
                        bulkResponse.hasFailures());
            }
        }
        if (bulkRequest.numberOfActions() > 0) {
            bulkResponse = bulkRequest.setRefresh(rebuidIndex).get();
        }
        if (null != bulkResponse && !bulkResponse.hasFailures()) {
            return true;
        } else {
            throw new SearchRuntimeException("insert error", gson.toJson(bulkResponse));
        }
    }

    @Override
    public boolean bulkMapUpdate(List<String> ids, List<Map<String, Object>> datas) {
        return bulkMapUpdate(ids, datas, true);
    }

    @Override
    public boolean bulkMapUpdate(List<String> ids, List<Map<String, Object>> datas, boolean rebuidIndex) {
        if (null == ids || null == datas || ids.size() != datas.size())
            throw new SearchRuntimeException("bulk update Null parameters or size not equal!");
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        int i = 0;
        for (String documentId : ids) {
            bulkRequestBuilder.add(client.prepareUpdate(indexName, indexName, documentId).setUpsert(datas.get(i))
                    .setDoc(datas.get(i++)));
        }

        BulkResponse bulkResponse = bulkRequestBuilder.setRefresh(rebuidIndex).get();
        if (!bulkResponse.hasFailures())
            return true;
        else {
            logBulkRespone(bulkResponse);
            throw new SearchRuntimeException("bulk documents update error", gson.toJson(bulkResponse));
        }
    }

    @Override
    public boolean bulkJsonUpdate(List<String> ids, List<String> jsons) {
        return bulkJsonUpdate(ids, jsons, true);
    }

    @Override
    public boolean bulkJsonUpdate(List<String> ids, List<String> jsons, boolean rebuidIndex) {
        if (null == ids || null == jsons || ids.size() != jsons.size())
            throw new SearchRuntimeException("bulk json update Null parameters or size not equal!");
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        int i = 0;
        for (String documentId : ids) {
            bulkRequestBuilder.add(client.prepareUpdate(indexName, indexName, documentId).setUpsert(jsons.get(i))
                    .setDoc(jsons.get(i++)));
        }

        BulkResponse bulkResponse = bulkRequestBuilder.setRefresh(rebuidIndex).get();
        if (!bulkResponse.hasFailures())
            return true;
        else {
            logBulkRespone(bulkResponse);
            throw new SearchRuntimeException("bulk json update error", gson.toJson(bulkResponse));
        }
    }

    @Override
    public <T> boolean bulkUpdate(List<String> ids, List<T> datas) {
        return bulkUpdate(ids, datas, true);
    }

    @Override
    public <T> boolean bulkUpdate(List<String> ids, List<T> datas, boolean rebuidIndex) {
        if (null == ids || null == datas || ids.size() != datas.size())
            throw new SearchRuntimeException("Null parameters or size not equal!");
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        int i = 0;
        for (String documentId : ids) {
            bulkRequestBuilder.add(client.prepareUpdate(indexName, indexName, documentId)
                    .setUpsert(esgson.toJson(datas.get(i))).setDoc(esgson.toJson(datas.get(i++))));
        }

        BulkResponse bulkResponse = bulkRequestBuilder.setRefresh(rebuidIndex).get();
        if (!bulkResponse.hasFailures())
            return true;
        else {
            logBulkRespone(bulkResponse);
            throw new SearchRuntimeException("batch object update error", gson.toJson(bulkResponse));
        }
    }

    @Override
    public boolean bulkUpdate(List<String> ids, Set<JsonBuilder> jsonBuilders) {
        return bulkUpdate(ids, jsonBuilders, true);
    }

    @Override
    public boolean bulkUpdate(List<String> ids, Set<JsonBuilder> jsonBuilders, boolean rebuidIndex) {
        if (null == ids || null == jsonBuilders || ids.size() != jsonBuilders.size())
            throw new SearchRuntimeException("Null parameters or size not equal!");
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        int i = 0;
        for (JsonBuilder jsonBuilder : jsonBuilders) {
            bulkRequestBuilder.add(client.prepareUpdate(indexName, indexName, ids.get(i++))
                    .setUpsert(jsonBuilder.getBuilder()).setDoc(jsonBuilder.getBuilder()));
        }
        BulkResponse bulkResponse = bulkRequestBuilder.setRefresh(rebuidIndex).get();
        if (!bulkResponse.hasFailures())
            return true;
        else {
            logBulkRespone(bulkResponse);
            throw new SearchRuntimeException("update error", gson.toJson(bulkResponse));
        }
    }

    @Override
    public boolean bulkMapUpsert(List<String> ids, List<Map<String, Object>> datas) {
        return bulkMapUpdate(ids, datas);
    }

    @Override
    public boolean bulkMapUpsert(List<String> ids, List<Map<String, Object>> datas, boolean rebuidIndex) {
        return bulkMapUpdate(ids, datas, rebuidIndex);
    }

    @Override
    public boolean bulkJsonUpsert(List<String> ids, List<String> jsons) {
        return bulkJsonUpdate(ids, jsons);
    }

    @Override
    public boolean bulkJsonUpsert(List<String> ids, List<String> jsons, boolean rebuidIndex) {
        return bulkJsonUpdate(ids, jsons, rebuidIndex);
    }

    @Override
    public <T> boolean bulkUpsert(List<String> ids, List<T> datas) {
        return bulkUpdate(ids, datas);
    }

    @Override
    public <T> boolean bulkUpsert(List<String> ids, List<T> datas, boolean rebuidIndex) {
        return bulkUpdate(ids, datas, rebuidIndex);
    }

    @Override
    public boolean bulkUpsert(List<String> ids, Set<JsonBuilder> jsonBuilders) {
        return bulkUpsert(ids, jsonBuilders);
    }

    @Override
    public boolean bulkUpsert(List<String> ids, Set<JsonBuilder> jsonBuilders, boolean rebuidIndex) {
        return bulkUpsert(ids, jsonBuilders, rebuidIndex);
    }

    private <T> Result<T> search(QueryBuilder queryBuilder, int from, int offset, List<Sort> sorts, Class<T> clazz,
            List<SearchCriteria> searchCriterias, String[] resultFields) {
        return search(queryBuilder, from, offset, sorts, clazz, null, searchCriterias, resultFields);
    }

    private <T> Result<T> search(QueryBuilder queryBuilder, int from, int offset, List<Sort> sorts, Class<T> clazz,
            @SuppressWarnings("rawtypes") TypeGetter typeGetter, List<SearchCriteria> searchCriterias,
            String[] resultFields) {
        Result<T> result = new Result<>();
        result.setResultCode(PaaSConstant.ExceptionCode.SYSTEM_ERROR);
        try {
            /* 查询搜索总数 */
            // 此种实现不好，查询两次。即使分页，也可以得到总数

            SearchRequestBuilder searchRequestBuilder = null;
            searchRequestBuilder = client.prepareSearch(indexName).setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setScroll(new TimeValue(60000)).setQuery(queryBuilder).setSize(100).setExplain(true);
            if (sorts == null || sorts.isEmpty()) {
                /* 如果不需要排序 */
            } else {
                /* 如果需要排序 */
                for (Sort sort : sorts) {
                    // 优先使用原生的搜索条件--gucl--20190219
                    if (sort.getSortBuilder() != null) {
                        searchRequestBuilder = searchRequestBuilder.addSort(sort.getSortBuilder());
                    } else {
                        SortOrder sortOrder = sort.getOrder().compareTo(Sort.SortOrder.DESC) == 0 ? SortOrder.DESC
                                : SortOrder.ASC;

                        searchRequestBuilder = searchRequestBuilder.addSort(sort.getSortBy(), sortOrder);
                    }
                }
            }
            // 增加高亮
            if (null != searchCriterias) {
                searchRequestBuilder = searchHelper.createHighlight(searchRequestBuilder, searchCriterias,
                        highlightCSS);
            }
            logger.info("--ES search:\r\n{}", searchRequestBuilder);
            SearchResponse searchResponse;
            if (null == resultFields || resultFields.length == 0)
                searchResponse = searchRequestBuilder.get();
            else
                searchResponse = searchRequestBuilder.setFetchSource(resultFields, null).get();
            List<T> list = searchHelper.getSearchResult(client, searchResponse, clazz, typeGetter, from, offset, sorts);

            result.setContents(list);
            result.setCounts(searchResponse.getHits().getTotalHits());
            result.setResultCode(PaaSConstant.RPC_CALL_OK);
        } catch (Exception e) {
            throw new SearchRuntimeException("ES searchIndex error!", e);
        }
        return result;
    }

    @Override
    public <T> Result<T> searchBySQL(String query, int from, int offset, List<Sort> sorts, Class<T> clazz) {
        // 创建query
        QueryBuilder queryBuilder = searchHelper.createStringSQLBuilder(query);
        return search(queryBuilder, from, offset, sorts, clazz, null, null);
    }

    @Override
    public <T> Result<T> searchBySQL(String querySQL, int from, int offset, List<Sort> sorts, Class<T> clazz,
            String[] resultFields) {
        // 创建query
        QueryBuilder queryBuilder = searchHelper.createStringSQLBuilder(querySQL);
        return search(queryBuilder, from, offset, sorts, clazz, null, resultFields);
    }

    @Override
    public String searchBySQL(String querySQL, int from, int offset, List<Sort> sorts) {
        return gson.toJson(searchBySQL(querySQL, from, offset, sorts, String.class));
    }

    @Override
    public String searchBySQL(String querySQL, int from, int offset, List<Sort> sorts, String[] resultFields) {
        return gson.toJson(searchBySQL(querySQL, from, offset, sorts, String.class, resultFields));
    }

    @Override
    public <T> Result<T> search(List<SearchCriteria> searchCriterias, int from, int offset, List<Sort> sorts,
            Class<T> clazz) {
        // 创建query
        QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
        return search(queryBuilder, from, offset, sorts, clazz, searchCriterias, null);
    }

    @Override
    public <T> Result<T> search(List<SearchCriteria> searchCriterias, int from, int offset, List<Sort> sorts,
            Class<T> clazz, String[] resultFields) {
        // 创建query
        QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
        return search(queryBuilder, from, offset, sorts, clazz, searchCriterias, resultFields);
    }

    @Override
    public String search(List<SearchCriteria> searchCriterias, int from, int offset, List<Sort> sorts) {
        return gson.toJson(search(searchCriterias, from, offset, sorts, String.class));
    }

    @Override
    public String search(List<SearchCriteria> searchCriterias, int from, int offset, List<Sort> sorts,
            String[] resultFields) {
        return gson.toJson(search(searchCriterias, from, offset, sorts, String.class, resultFields));
    }

    public <T> Result<T> searchByDSL(String dslJson, int from, int offset, @Nullable List<Sort> sorts, Class<T> clazz) {
        QueryBuilder queryBuilder = QueryBuilders.wrapperQuery(dslJson);
        return search(queryBuilder, from, offset, sorts, clazz, null, null);
    }

    @Override
    public <T> Result<T> searchByDSL(String dslJson, int from, int offset, List<Sort> sorts, Class<T> clazz,
            String[] resultFields) {
        QueryBuilder queryBuilder = QueryBuilders.wrapperQuery(dslJson);
        return search(queryBuilder, from, offset, sorts, clazz, null, resultFields);
    }

    @Override
    public String searchByDSL(String dslJson, int from, int offset, List<Sort> sorts) {
        return gson.toJson(searchByDSL(dslJson, from, offset, sorts, String.class));
    }

    @Override
    public String searchByDSL(String dslJson, int from, int offset, List<Sort> sorts, String[] resultFields) {
        return gson.toJson(searchByDSL(dslJson, from, offset, sorts, String.class, resultFields));
    }

    @Override
    public Result<Map<String, Long>> aggregate(List<SearchCriteria> searchCriterias, String field) {
        Result<Map<String, Long>> result = new Result<>();
        result.setResultCode(PaaSConstant.ExceptionCode.SYSTEM_ERROR);
        try {
            QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
            if (queryBuilder == null)
                return result;
            SearchResponse searchResponse = client.prepareSearch(indexName).setSearchType(SearchType.DEFAULT)
                    .setQuery(queryBuilder)
                    .addAggregation(AggregationBuilders.terms(field + "_aggs").field(field + ".raw").size(100))
                    .setSize(0).get();

            Terms sortAggregate = searchResponse.getAggregations().get(field + "_aggs");
            for (Terms.Bucket entry : sortAggregate.getBuckets()) {
                result.addAgg(new AggResult(entry.getKeyAsString(), entry.getDocCount(), field));
            }
            result.setCounts(searchResponse.getHits().getTotalHits());
            result.setResultCode(PaaSConstant.RPC_CALL_OK);
        } catch (Exception e) {
            throw new SearchRuntimeException("ES aggregation error", e);
        }
        return result;
    }

    @Override
    public Result<List<AggResult>> aggregate(List<SearchCriteria> searchCriterias, List<AggField> fields) {
        if (null == searchCriterias || null == fields || searchCriterias.isEmpty() || fields.isEmpty())
            throw new SearchRuntimeException("Illegel Arguments! null");
        Result<List<AggResult>> result = new Result<>();
        result.setResultCode(PaaSConstant.ExceptionCode.SYSTEM_ERROR);
        try {
            QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
            if (queryBuilder == null)
                return result;
            result.setResultCode(PaaSConstant.RPC_CALL_OK);
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                    .setSearchType(SearchType.DEFAULT).setQuery(queryBuilder);

            for (AggField aggField : fields) {
                // 先建第一级
                TermsBuilder termBuilder = AggregationBuilders.terms(aggField.getField() + "_aggs")
                        .field(aggField.getField() + ".raw").size(0);
                // 循环创建子聚合
                termBuilder = searchHelper.addSubAggs(termBuilder, aggField.getSubAggs());
                searchRequestBuilder.addAggregation(termBuilder);
            }
            SearchResponse searchResponse = searchRequestBuilder.setSize(0).get();

            result.setCounts(searchResponse.getHits().getTotalHits());
            result.setAggs(searchHelper.getAgg(searchResponse, fields));
        } catch (Exception e) {
            throw new SearchRuntimeException("aggregation error", e);
        }
        return result;
    }

    @Override
    public <T> Result<T> fullTextSearch(String text, int from, int offset, List<Sort> sorts, Class<T> clazz) {
        Result<T> result = new Result<>();
        result.setResultCode(PaaSConstant.ExceptionCode.SYSTEM_ERROR);
        try {
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                    .setSearchType(SearchType.QUERY_THEN_FETCH).setScroll(new TimeValue(60000))
                    .setQuery(QueryBuilders.matchQuery("_all", text).operator(Operator.AND).minimumShouldMatch("75%"))
                    .setSize(100).setExplain(true).setHighlighterRequireFieldMatch(true);
            SearchResponse response = searchRequestBuilder.get();

            List<T> list = searchHelper.getSearchResult(client, response, clazz, null, from, offset, sorts);

            result.setContents(list);
            result.setCounts(response.getHits().totalHits());

            result.setResultCode(PaaSConstant.RPC_CALL_OK);
        } catch (Exception e) {
            throw new SearchRuntimeException("ES searchIndex error", e);
        }
        return result;
    }

    @Override
    public <T> Result<T> fullTextSearch(String text, List<String> qryFields, List<AggField> aggFields, int from,
            int offset, List<Sort> sorts, Class<T> clazz) {
        Result<T> result = new Result<>();
        result.setResultCode(PaaSConstant.ExceptionCode.SYSTEM_ERROR);
        try {
            // 如果带聚合必须指定对哪些字段
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            for (String qryField : qryFields) {
                queryBuilder.should(
                        QueryBuilders.matchQuery(qryField, text).operator(Operator.AND).minimumShouldMatch("75%"));
            }
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                    .setSearchType(SearchType.QUERY_THEN_FETCH).setScroll(new TimeValue(60000)).setQuery(queryBuilder)
                    .setSize(100);
            if (sorts == null || sorts.isEmpty()) {
                /* 如果不需要排序 */
            } else {
                /* 如果需要排序 */
                for (Sort sort : sorts) {
                    SortOrder sortOrder = sort.getOrder().compareTo(Sort.SortOrder.DESC) == 0 ? SortOrder.DESC
                            : SortOrder.ASC;

                    searchRequestBuilder = searchRequestBuilder.addSort(sort.getSortBy(), sortOrder);
                }
            }
            searchRequestBuilder.setExplain(false).setHighlighterRequireFieldMatch(true);
            // 此处加上聚合内容
            for (AggField aggField : aggFields) {
                // 先建第一级
                TermsBuilder termBuilder = AggregationBuilders.terms(aggField.getField() + "_aggs")
                        .field(aggField.getField() + ".raw").size(0);
                // 循环创建子聚合
                termBuilder = searchHelper.addSubAggs(termBuilder, aggField.getSubAggs());
                searchRequestBuilder.addAggregation(termBuilder);
            }
            SearchResponse response = searchRequestBuilder.get();

            List<T> list = searchHelper.getSearchResult(client, response, clazz, null, from, offset, sorts);

            result.setContents(list);
            result.setCounts(response.getHits().totalHits());
            result.setAggs(searchHelper.getAgg(response, aggFields));
            result.setResultCode(PaaSConstant.RPC_CALL_OK);
        } catch (Exception e) {
            throw new SearchRuntimeException("ES searchIndex error", e);
        }
        return result;
    }

    @Override
    public <T> T getById(String id, Class<T> clazz) {
        GetResponse response = client.prepareGet(indexName, indexName, id).get();
        if (null != response && response.isExists()) {
            return esgson.fromJson(response.getSourceAsString(), clazz);
        } else
            return null;
    }

    @Override
    public String getById(String id) {
        GetResponse response = client.prepareGet(indexName, indexName, id).get();
        if (null != response && response.isExists())
            return response.getSourceAsString();
        else
            return null;
    }

    @Override
    public boolean createIndex(String indexName, int shards, int replicas) {
        if (shards <= 0)
            shards = 1;
        if (replicas <= 0)
            replicas = 1;
        String setting = " {" + "\"number_of_shards\":\"" + shards + "\"," + "\"number_of_replicas\":\"" + replicas
                + "\"," + "\"client.transport.ping_timeout\":\"60s\"," + " \"analysis\": {" + "         \"filter\": {"
                + "            \"nGram_filter\": {" + "               \"type\": \"nGram\","
                + "               \"min_gram\": 1," + "               \"max_gram\": 10" + "            }"
                + "         }," + "         \"analyzer\": {" + "            \"nGram_analyzer\": {"
                + "               \"type\": \"custom\"," + "               \"tokenizer\": \"ik_max_word\","
                + "               \"filter\": [" + "                  \"stop\"," + "                  \"nGram_filter\""
                + "               ]" + "            }" + "         }" + "      }" + "   " + "}";

        CreateIndexResponse createResponse = client.admin().indices().prepareCreate(indexName).setSettings(setting)
                .get();
        return createResponse.isAcknowledged();
    }

    @Override
    public boolean deleteIndex(String indexName) {
        DeleteIndexResponse delete;
        try {
            delete = client.admin().indices().delete(new DeleteIndexRequest(indexName)).get();
            if (!delete.isAcknowledged()) {
                logger.error("Index wasn't deleted!index={}", indexName);
                return false;
            }
            return true;
        } catch (Exception e) {
            throw new SearchRuntimeException("ES delete index error", e);
        }
    }

    @Override
    public boolean addMapping(String indexName, String type, String json) {
        return addMapping(indexName, type, json, true);
    }

    @Override
    public boolean existIndex(String indexName) {
        Assert.notNull(indexName, "Index Name can not be null");
        try {
            IndicesExistsResponse response = client.admin().indices().exists(new IndicesExistsRequest(indexName)).get();
            return null != response && response.isExists();
        } catch (Exception e) {
            throw new SearchRuntimeException("ES delete index error", e);
        }
    }

    @Override
    public boolean refresh() {
        RefreshResponse response = client.admin().indices().prepareRefresh(indexName).get();
        return null != response && response.getFailedShards() <= 0;
    }

    @Override
    public <T> Result<T> fullTextSearch(String text, List<AggField> aggFields, int from, int offset, List<Sort> sorts,
            Class<T> clazz) {
        List<String> qryFields = new ArrayList<>();
        qryFields.add("_all");
        return fullTextSearch(text, qryFields, aggFields, from, offset, sorts, clazz);
    }

    @Override
    public void close() {
        if (null != client) {
            client.close();
            client = null;
        }
    }

    @Override
    public boolean addMapping(String indexName, String type, String json, String id) {
        if (!StringUtil.isBlank(id))
            this.id = id;
        return addMapping(indexName, type, json);
    }

    @Override
    public boolean createIndex(String indexName, String settings) {
        CreateIndexResponse createResponse = client.admin().indices().prepareCreate(indexName).setSettings(settings)
                .get();
        return createResponse.isAcknowledged();
    }

    @Override
    public boolean existMapping(String indexName, String mapping) {
        GetMappingsResponse response = client.admin().indices().prepareGetMappings(indexName).get();
        if (null != response) {
            return response.getMappings().containsKey(mapping);
        }
        return false;
    }

    @Override
    public boolean addMapping(String indexName, String type, String json, boolean addDynamicTemplate) {
        // 这里要做些处理，如果用户没有type,或者对应不上应该报错
        Assert.notNull(indexName, "Index Name can not input null");
        Assert.notNull(type, "type can not null");
        Assert.notNull(json, "mapping define can not null");
        // 转换成json看看
        JsonObject typeObj = null;
        JsonObject jsonObj = esgson.fromJson(json, JsonObject.class);
        if (null == jsonObj.get(type)) {
            // 看看有没有properties
            // 这里好办了,补上两层
            JsonObject properties = new JsonObject();
            properties.add("properties", jsonObj);
            if (addDynamicTemplate) {
                // 增加动态mapping分词模板,对于所有的字符串应用分词
                String dynamicTemplate = "{ \"ik\": {" + "\"match\":              \"*\","
                        + "  \"match_mapping_type\": \"string\"," + "  \"mapping\": {"
                        + "      \"type\":           \"string\"," + "      \"analyzer\":       \"ik_max_word\"" + "  }"
                        + " }}";
                JsonObject dynamicT = esgson.fromJson(dynamicTemplate, JsonObject.class);
                JsonArray dynamicTemplates = new JsonArray();
                dynamicTemplates.add(dynamicT);
                properties.add("dynamic_templates", dynamicTemplates);
            }
            typeObj = new JsonObject();
            typeObj.add(type, properties);

            // 这里也好办了,补上一层
        } else {
            // 存在就看自己是否正确构造
            typeObj = jsonObj;
        }

        PutMappingResponse putMappingResponse = client.admin().indices().preparePutMapping(indexName).setType(type)
                .setSource(esgson.toJson(typeObj)).get();
        return putMappingResponse.isAcknowledged();
    }

    @Override
    public <T> Result<T> search(List<SearchCriteria> searchCriterias, int from, int offset, List<Sort> sorts,
            @SuppressWarnings("rawtypes") TypeGetter typeGetter, String[] resultFields) {
        QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
        return search(queryBuilder, from, offset, sorts, null, typeGetter, searchCriterias, resultFields);
    }

    @Override
    public <T> Result<T> search(List<SearchCriteria> searchCriterias, int from, int offset, List<Sort> sorts,
            @SuppressWarnings("rawtypes") TypeGetter typeGetter) {
        QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
        return search(queryBuilder, from, offset, sorts, null, typeGetter, searchCriterias, null);
    }

    @Override
    public boolean addMapping(String indexName, String type, String json, List<DynamicMatchOption> matchs) {
        // 这里要做些处理，如果用户没有type,或者对应不上应该报错
        Assert.notNull(indexName, "Index Name can not null");
        Assert.notNull(type, "type can not null");
        Assert.notNull(json, "mapping can not null");
        // 转换成json看看
        JsonObject typeObj = null;
        JsonObject jsonObj = esgson.fromJson(json, JsonObject.class);
        if (null == jsonObj.get(type)) {
            // 看看有没有properties
            // 这里好办了,补上两层
            JsonObject properties = new JsonObject();
            properties.add("properties", jsonObj);
            if (null != matchs) {
                JsonArray dynamicTemplates = new JsonArray();
                StringBuilder sb = new StringBuilder();
                for (DynamicMatchOption match : matchs) {
                    sb.delete(0, sb.length());
                    sb.append("{ \"").append(match.getName()).append("\": {");
                    sb.append("\"match_mapping_type\": \"string\",");
                    switch (match.getMatchType()) {
                    case PATTERN:
                        sb.append("\"match_pattern\": \"regex\",");
                        sb.append("\"match\":\"");
                        sb.append(match.getMatch()).append("\",");
                        break;
                    case PATH:
                        sb.append("\"path_match\":\"").append(match.getMatch()).append("\",");
                        if (!StringUtil.isBlank(match.getUnmatch()))
                            sb.append("\"path_unmatch\":\"").append(match.getMatch()).append("\",");
                        break;
                    default:
                        sb.append("\"match\":\"").append(match.getMatch()).append("\",");
                    }
                    sb.append("  \"mapping\": {" + "      \"type\":           \"string\",");
                    if (match.isAnalyzed()) {
                        sb.append("      \"analyzer\":       \"");
                        sb.append("ik_max_word\"");
                    } else {
                        sb.append("      \"index\":       \"");
                        sb.append("not_analyzed\"");
                    }
                    sb.append("   } }}");
                    logger.info("dynamic mapping:\r\n{}", sb);
                    JsonObject dynamicT = esgson.fromJson(sb.toString(), JsonObject.class);
                    dynamicTemplates.add(dynamicT);
                }
                properties.add("dynamic_templates", dynamicTemplates);
            }
            typeObj = new JsonObject();
            typeObj.add(type, properties);

            // 这里也好办了,补上一层
        } else {
            // 存在就看自己是否正确构造
            typeObj = jsonObj;
        }

        PutMappingResponse putMappingResponse = client.admin().indices().preparePutMapping(indexName).setType(type)
                .setSource(esgson.toJson(typeObj)).get();
        logger.info("add mapping:\r\n{}", typeObj);
        return putMappingResponse.isAcknowledged();
    }

    @Override
    public boolean setRefeshTime(long seconds) {
        Settings localSettings = Settings.settingsBuilder().put("index.refresh_interval", seconds + "s").build();

        UpdateSettingsResponse usrp = client.admin().indices().prepareUpdateSettings().setIndices(indexName)
                .setSettings(localSettings).get();
        return usrp.isAcknowledged();
    }

    @Override
    public boolean addMapping(String indexName, String json) {
        // 这里要做些处理，如果用户没有type,或者对应不上应该报错
        Assert.notNull(indexName, "Index Name can not null");
        Assert.notNull(json, "mapping can not null");
        JsonObject jsonObj = esgson.fromJson(json, JsonObject.class);
        String type = null;
        Iterator<Entry<String, JsonElement>> iter = jsonObj.entrySet().iterator();
        while (iter.hasNext()) {
            type = iter.next().getKey();
            break;
        }
        PutMappingResponse putMappingResponse = client.admin().indices().preparePutMapping(indexName).setType(type)
                .setSource(json).get();
        logger.info("add mapping:\r\n{}", json);
        return putMappingResponse.isAcknowledged();
    }

    @Override
    public StatResult count(List<SearchCriteria> searchCriterias, String field) {
        if (null == searchCriterias || StringUtil.isBlank(field) || searchCriterias.isEmpty())
            throw new SearchRuntimeException("parameters is null");
        StatResult result = new StatResult();
        try {
            QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
            if (queryBuilder == null)
                return result;
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                    .setSearchType(SearchType.DEFAULT).setQuery(queryBuilder);

            ValueCountBuilder vcb = AggregationBuilders.count("agg").field(field).missing(0);
            searchRequestBuilder.addAggregation(vcb);
            logger.info("--ES count search:\r\n{}", searchRequestBuilder);
            SearchResponse searchResponse = searchRequestBuilder.setSize(0).get();

            ValueCount agg = searchResponse.getAggregations().get("agg");
            long value = agg.getValue();
            result.setCount(value);
        } catch (Exception e) {
            throw new SearchRuntimeException("ES simple Aggregation error", e);
        }
        return result;
    }

    @Override
    public StatResult stat(List<SearchCriteria> searchCriterias, String field) {
        if (null == searchCriterias || StringUtil.isBlank(field) || searchCriterias.isEmpty())
            throw new SearchRuntimeException("stat parameters can not null!");
        StatResult result = new StatResult();
        try {
            QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
            if (queryBuilder == null)
                return result;
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                    .setSearchType(SearchType.DEFAULT).setQuery(queryBuilder);

            StatsBuilder sb = AggregationBuilders.stats("agg").field(field).missing(0);
            searchRequestBuilder.addAggregation(sb);
            logger.info("--ES stat search:\r\n{}", searchRequestBuilder);
            SearchResponse searchResponse = searchRequestBuilder.setSize(0).get();

            Stats agg = searchResponse.getAggregations().get("agg");
            result.setCount(agg.getCount());
            result.setMin(agg.getMin());
            result.setMax(agg.getMax());
            result.setAvg(agg.getAvg());
            result.setSum(agg.getSum());
            result.setMinTxt(agg.getAvgAsString());
            result.setMaxTxt(agg.getMaxAsString());
            result.setAvgTxt(agg.getAvgAsString());
            result.setSumTxt(agg.getSumAsString());
        } catch (Exception e) {
            throw new SearchRuntimeException("ES simpleAggregation error", e);
        }
        return result;
    }

    @Override
    public List<StatResult> stat(List<SearchCriteria> searchCriterias, String field, String groupBy) {
        if (null == searchCriterias || StringUtil.isBlank(field) || StringUtil.isBlank(groupBy)
                || searchCriterias.isEmpty())
            throw new SearchRuntimeException("IllegelArguments! null");
        List<StatResult> results = new ArrayList<>();
        try {
            QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
            if (queryBuilder == null)
                return results;
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                    .setSearchType(SearchType.DEFAULT).setQuery(queryBuilder);

            TermsBuilder tb = AggregationBuilders.terms("groupAggs").field(groupBy)
                    .subAggregation(AggregationBuilders.stats("agg").field(field).missing(0));
            searchRequestBuilder.addAggregation(tb);
            logger.info("--ES stat search:\r\n{}", searchRequestBuilder);
            SearchResponse searchResponse = searchRequestBuilder.setSize(0).get();
            // 得到所有桶
            Terms aggs = searchResponse.getAggregations().get("groupAggs");
            if (null == aggs)
                return results;
            StatResult sr = null;
            for (Terms.Bucket entry : aggs.getBuckets()) {
                sr = new StatResult();
                sr.setGroupField(groupBy);
                sr.setGroupKey(entry.getKeyAsString());
                Stats agg = entry.getAggregations().get("agg");
                sr.setCount(agg.getCount());
                sr.setMin(agg.getMin());
                sr.setMax(agg.getMax());
                sr.setAvg(agg.getAvg());
                sr.setSum(agg.getSum());
                sr.setMinTxt(agg.getAvgAsString());
                sr.setMaxTxt(agg.getMaxAsString());
                sr.setAvgTxt(agg.getAvgAsString());
                sr.setSumTxt(agg.getSumAsString());
                results.add(sr);
            }
        } catch (Exception e) {
            throw new SearchRuntimeException("ES simpleAggregation error", e);
        }
        return results;
    }

    @Override
    public List<StatResult> count(List<SearchCriteria> searchCriterias, String field, String groupBy) {
        if (null == searchCriterias || StringUtil.isBlank(field) || StringUtil.isBlank(groupBy)
                || searchCriterias.isEmpty())
            throw new SearchRuntimeException("IllegelArguments! null");
        List<StatResult> results = new ArrayList<>();
        try {
            QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
            if (queryBuilder == null)
                return results;
            SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
                    .setSearchType(SearchType.DEFAULT).setQuery(queryBuilder);

            TermsBuilder tb = AggregationBuilders.terms("groupAggs").field(groupBy)
                    .subAggregation(AggregationBuilders.count("agg").field(field).missing(0));
            searchRequestBuilder.addAggregation(tb);
            logger.info("--ES count search:\r\n{}", searchRequestBuilder);
            SearchResponse searchResponse = searchRequestBuilder.setSize(0).get();
            // 得到所有桶
            Terms aggs = searchResponse.getAggregations().get("groupAggs");
            if (null == aggs)
                return results;
            StatResult sr = null;
            for (Terms.Bucket entry : aggs.getBuckets()) {
                sr = new StatResult();
                sr.setGroupField(groupBy);
                sr.setGroupKey(entry.getKeyAsString());
                ValueCount vc = entry.getAggregations().get("agg");
                sr.setCount(vc.getValue());
                results.add(sr);
            }
        } catch (Exception e) {
            throw new SearchRuntimeException("ES simpleAggregation error", e);
        }
        return results;
    }

}
