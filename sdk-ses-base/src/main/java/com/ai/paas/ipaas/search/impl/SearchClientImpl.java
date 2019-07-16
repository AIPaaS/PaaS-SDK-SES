package com.ai.paas.ipaas.search.impl;

import java.io.IOException;

//搜索实现定义

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.metrics.StatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.aggregations.metrics.ValueCountAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.Constant;
import com.ai.paas.ipaas.search.ISearchClient;
import com.ai.paas.ipaas.search.SearchRuntimeException;
import com.ai.paas.ipaas.search.common.DynamicMatchOption;
import com.ai.paas.ipaas.search.common.JsonBuilder;
import com.ai.paas.ipaas.search.common.TypeGetter;
import com.ai.paas.ipaas.search.vo.AggField;
import com.ai.paas.ipaas.search.vo.AggResult;
import com.ai.paas.ipaas.search.vo.GeoLocation;
import com.ai.paas.ipaas.search.vo.Result;
import com.ai.paas.ipaas.search.vo.SearchCriteria;
import com.ai.paas.ipaas.search.vo.Sort;
import com.ai.paas.ipaas.search.vo.StatResult;
import com.ai.paas.util.Assert;
import com.ai.paas.util.StringUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class SearchClientImpl implements ISearchClient {
    private Logger logger = LoggerFactory.getLogger(SearchClientImpl.class);
    private String highlightCSS = "span,span";
    private String indexName;
    private String id = null;
    private String hosts = null;
    private String userName = null;
    private String passwd = null;
    private final String groupAggName = "groupAggs";

    // 创建私有对象
    private RestHighLevelClient client;

    private String esDateFmt = "yyyy-MM-dd'T'HH:mm:ssZZZ";
    private Gson esgson = new GsonBuilder().setDateFormat(esDateFmt).create();
    private Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
    private static final int BATCH_SIZE = 1000;
    private SearchHelper searchHelper = new SearchHelper();
    // 单位毫秒
    private int connectTimeOut = 60000;
    private int socketTimeOut = 60000;
    private int connectionRequestTimeOut = 600000;

    public SearchClientImpl(String hosts, String indexName, String id) {
        this.indexName = indexName;
        this.id = id;
        this.hosts = hosts;
        initClient();
        searchHelper.setDateFmt(this.esDateFmt);
    }

    public SearchClientImpl(String hosts, String indexName, String id, String userName, String passwd) {
        this.indexName = indexName;
        this.id = id;
        this.hosts = hosts;
        this.userName = userName;
        this.passwd = passwd;
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
        if (null != client)
            return;
        List<String> clusterList = new ArrayList<>();
        try {
            if (!StringUtil.isBlank(hosts)) {
                clusterList = Arrays.asList(hosts.split(","));
            }
            List<HttpHost> hostList = new ArrayList<>();
            for (String item : clusterList) {
                String address = item.split(":")[0];
                int port = Integer.parseInt(item.split(":")[1]);
                /* 通过tcp连接搜索服务器，如果连接不上，有一种可能是服务器端与客户端的jar包版本不匹配 */
                hostList.add(new HttpHost(address, port));
            }
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(this.userName, this.passwd));
            HttpHost[] restHosts = hostList.toArray(new HttpHost[hostList.size()]);
            client = new RestHighLevelClient(RestClient.builder(restHosts)
                    .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                        @Override
                        public Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                            requestConfigBuilder.setConnectTimeout(connectTimeOut);
                            requestConfigBuilder.setSocketTimeout(socketTimeOut);
                            requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeOut);
                            return requestConfigBuilder;
                        }
                    }).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                            if (StringUtil.isBlank(userName))
                                return null;

                            return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    }));
        } catch (Exception e) {
            throw new SearchRuntimeException("ES init client error", e);
        }

    }

    public void setHighlightCSS(String highlightCSS) {
        this.highlightCSS = highlightCSS;
    }

    @SuppressWarnings("rawtypes")
    public List<String> getSuggest(String field, String value, int count) {
        List<String> suggests = new ArrayList<>();
        if (StringUtil.isBlank(field) || StringUtil.isBlank(value) || count <= 0)
            return suggests;

        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SuggestionBuilder termSuggestionBuilder = SuggestBuilders.completionSuggestion(field).prefix(value).text(value)
                .size(count);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion(field + "-suggest", termSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);
        searchRequest.source(searchSourceBuilder);
        logger.info("es suggestion:\r\n{}", Strings.toString(suggestBuilder));
        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new SearchRuntimeException("suggest error!", e);
        }
        if (null == response)
            return suggests;

        CompletionSuggestion termSuggestion = response.getSuggest().getSuggestion(field + "-suggest");
        if (null == termSuggestion || null == termSuggestion.getOptions())
            return suggests;
        List<CompletionSuggestion.Entry.Option> options = termSuggestion.getOptions();
        for (CompletionSuggestion.Entry.Option entry : options) {
            suggests.add(entry.getText().toString());
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
        // 判断一下是否有id字段
        String parsedId = searchHelper.getId(json, this.id);
        IndexRequest request = new IndexRequest(indexName);
        if (!StringUtil.isBlank(parsedId)) {
            request.id(parsedId);
        }
        request.source(json, XContentType.JSON);
        // 可以等待刷新 request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);使用这句
        request.opType(DocWriteRequest.OpType.INDEX);
        return insert(request);
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
            String parsedId = searchHelper.getId(builder, id);
            IndexRequest request = new IndexRequest(indexName);
            if (!StringUtil.isBlank(parsedId)) {
                request.id(parsedId);
            }
            request.source(builder);
            // 可以等待刷新 request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);使用这句
            request.opType(DocWriteRequest.OpType.INDEX);
            return insert(request);
        } catch (Exception e) {
            throw new SearchRuntimeException(jsonBuilder.toString(), e);
        } finally {
            if (null != builder)
                builder.close();
        }
    }

    private boolean insert(IndexRequest request) {
        IndexResponse response = null;
        try {
            response = client.index(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new SearchRuntimeException("index error!{}", Strings.toString(request.source()), e);
        }
        if (null != response && !StringUtil.isBlank(response.getId())) {
            return true;
        } else {
            throw new SearchRuntimeException("index error!" + Strings.toString(request.source()),
                    gson.toJson(response));
        }
    }

    @Override
    public boolean delete(String id) {
        if (StringUtil.isBlank(id))
            throw new SearchRuntimeException("Illegel argument,id=" + id);
        DeleteRequest request = new DeleteRequest(indexName, id);
        DeleteResponse response = null;
        try {
            response = client.delete(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new SearchRuntimeException("id=" + id, e);
        }
        if (null != response && !StringUtil.isBlank(response.getId())) {
            return true;
        } else {
            throw new SearchRuntimeException("index error!" + id, gson.toJson(response));
        }
    }

    @Override
    public long bulkDelete(List<String> ids) {
        return bulkDelete(ids, true);
    }

    @Override
    public long bulkDelete(List<String> ids, boolean rebuildIndex) {
        if (null == ids || ids.isEmpty())
            return 0;
        BulkRequest bulkRequest = new BulkRequest();
        DeleteRequest request = null;
        for (String localId : ids) {
            request = new DeleteRequest(indexName, localId);
            bulkRequest.add(request);
        }
        if (rebuildIndex)
            bulkRequest.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        BulkResponse bulkResponse = null;
        try {
            bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new SearchRuntimeException("ids=" + ids, e);
        }
        if (!bulkResponse.hasFailures()) {
            return ids.size();
        } else {
            // 这里要做个日志，哪些成功了
            int failed = 0;
            for (BulkItemResponse response : bulkResponse.getItems()) {
                logger.error("Doc id:{} is falided:{}", response.getId(), response.isFailed());
                failed++;
            }
            return (ids.size() - failed);
        }
    }

    @Override
    public long delete(List<SearchCriteria> searchCriteria) {
        return delete(searchCriteria, true);
    }

    @Override
    public long delete(List<SearchCriteria> searchCriteria, boolean rebuidIndex) {
        if (null == searchCriteria || searchCriteria.isEmpty())
            return 0;

        DeleteByQueryRequest request = new DeleteByQueryRequest(indexName);
        request.setConflicts("proceed");
        request.setQuery(searchHelper.createQueryBuilder(searchCriteria));
        request.setSlices(5);
        request.setScroll(TimeValue.timeValueMinutes(30));
        request.setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        request.setRefresh(rebuidIndex);
        BulkByScrollResponse bulkResponse = null;
        try {
            bulkResponse = client.deleteByQuery(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new SearchRuntimeException("ids=" + searchCriteria, e);
        }
        return bulkResponse.getDeleted();
    }

    @Override
    public boolean clean() {
        DeleteByQueryRequest request = new DeleteByQueryRequest(indexName);
        request.setConflicts("proceed");
        request.setQuery(QueryBuilders.matchAllQuery());
        request.setSlices(5);
        request.setScroll(TimeValue.timeValueMinutes(120));
        request.setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        request.setRefresh(true);
        BulkByScrollResponse bulkResponse = null;
        try {
            bulkResponse = client.deleteByQuery(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new SearchRuntimeException("delte all documents error...", e);
        }
        return null != bulkResponse && bulkResponse.getBulkFailures().isEmpty();
    }

    private boolean update(UpdateRequest request) {
        UpdateResponse response = null;
        try {
            response = client.update(request, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new SearchRuntimeException("update documents error...{}", Strings.toString(request), e);
        }
        return !StringUtil.isBlank(response.getId());
    }

    @Override
    public boolean update(String id, Map<String, Object> data) {
        if (StringUtil.isBlank(id) || null == data || data.size() <= 0)
            return false;
        UpdateRequest request = new UpdateRequest(indexName, id);
        request.doc(data);
        return update(request);
    }

    @Override
    public boolean update(String id, String json) {
        if (StringUtil.isBlank(id) || StringUtil.isBlank(json))
            return false;
        UpdateRequest request = new UpdateRequest(indexName, id).doc(json, XContentType.JSON);
        return update(request);
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
        UpdateRequest request = new UpdateRequest(indexName, id).doc(jsonBuilder.getBuilder());
        return update(request);
    }

    @Override
    public boolean upsert(String id, Map<String, Object> data) {
        if (StringUtil.isBlank(id) || null == data || data.size() <= 0)
            return false;
        UpdateRequest request = new UpdateRequest(indexName, id).doc(data).upsert(data);
        return update(request);
    }

    @Override
    public boolean upsert(String id, String json) {
        if (StringUtil.isBlank(id) || StringUtil.isBlank(json))
            return false;
        UpdateRequest request = new UpdateRequest(indexName, id).doc(json, XContentType.JSON).upsert(json,
                XContentType.JSON);
        return update(request);
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
        UpdateRequest request = new UpdateRequest(indexName, id).doc(jsonBuilder.getBuilder())
                .upsert(jsonBuilder.getBuilder());
        return update(request);
    }

    @Override
    public void bulkMapInsert(List<Map<String, Object>> datas) {
        bulkMapInsert(datas, true);
    }

    @SuppressWarnings("rawtypes")
    private void bulkInsert(BulkRequest bulkRequest) {
        BulkProcessor bulkProcessor = searchHelper.init(client, BATCH_SIZE);
        List<DocWriteRequest<?>> requests = bulkRequest.requests();
        for (DocWriteRequest request : requests) {
            bulkProcessor.add(request);
        }
        bulkProcessor.flush();
        try {
            bulkProcessor.awaitClose(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            logger.error("bulk processor error!", e);
            Thread.currentThread().interrupt();
        }
        bulkProcessor.close();
    }

    @Override
    public void bulkMapInsert(List<Map<String, Object>> datas, boolean rebuidIndex) {
        if (null == datas || datas.isEmpty())
            return;
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest request = null;
        for (Map<String, Object> data : datas) {
            request = new IndexRequest(indexName);
            if (null != data.get(id)) {
                request.id((String) data.get(id));
            }
            request.source(data);
            bulkRequest.add(request);
        }
        bulkInsert(bulkRequest);
    }

    @Override
    public void bulkJsonInsert(List<String> jsons) {
        bulkJsonInsert(jsons, true);
    }

    @Override
    public void bulkJsonInsert(List<String> jsons, boolean rebuidIndex) {
        if (null == jsons || jsons.isEmpty())
            return;
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest request = null;
        for (String json : jsons) {
            request = new IndexRequest(indexName);
            if (searchHelper.hasId(json, id)) {
                request.id(searchHelper.getId(json, id));
            }
            request.source(json, XContentType.JSON);
            bulkRequest.add(request);
        }
        bulkInsert(bulkRequest);
    }

    @Override
    public <T> void bulkInsert(List<T> datas) {
        bulkInsert(datas, true);
    }

    @Override
    public <T> void bulkInsert(List<T> datas, boolean rebuidIndex) {
        if (null == datas || datas.isEmpty())
            return;
        List<String> jsons = new ArrayList<>();
        for (T t : datas) {
            jsons.add(esgson.toJson(t));
        }
        bulkJsonInsert(jsons, rebuidIndex);
    }

    @Override
    public void bulkInsert(Set<JsonBuilder> jsonBuilders) {
        bulkInsert(jsonBuilders, true);
    }

    @Override
    public void bulkInsert(Set<JsonBuilder> jsonBuilders, boolean rebuidIndex) {
        if (null == jsonBuilders || jsonBuilders.isEmpty())
            return;
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest request = null;
        for (JsonBuilder jsonBuilder : jsonBuilders) {
            request = new IndexRequest(indexName);
            if (searchHelper.hasId(jsonBuilder.getBuilder(), id)) {
                request.id(searchHelper.getId(jsonBuilder.getBuilder(), id));
            }
            request.source(jsonBuilder.getBuilder());
            bulkRequest.add(request);
        }
        bulkInsert(bulkRequest);
    }

    @Override
    public long bulkMapUpdate(List<String> ids, List<Map<String, Object>> datas) {
        return bulkMapUpdate(ids, datas, true);
    }

    private long bulkUpdate(BulkRequest bulkRequest) {
        BulkResponse bulkResponse = null;
        try {
            bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new SearchRuntimeException("update documents:{}" + bulkRequest, e);
        }
        if (!bulkResponse.hasFailures()) {
            return bulkRequest.numberOfActions();
        } else {
            // 这里要做个日志，哪些成功了
            int failed = 0;
            for (BulkItemResponse response : bulkResponse.getItems()) {
                logger.error("Doc id:{} is falided:{}", response.getId(), response.isFailed());
                failed++;
            }
            return (bulkRequest.numberOfActions() - failed);
        }
    }

    @Override
    public long bulkMapUpdate(List<String> ids, List<Map<String, Object>> datas, boolean rebuidIndex) {
        if (null == ids || null == datas || ids.size() != datas.size())
            throw new SearchRuntimeException("bulk update Null parameters or size not equal!");
        BulkRequest bulkRequest = new BulkRequest();
        int i = 0;
        UpdateRequest request = null;
        for (String documentId : ids) {
            request = new UpdateRequest(indexName, documentId).doc(datas.get(i)).upsert(datas.get(i));
            bulkRequest.add(request);
        }

        return bulkUpdate(bulkRequest);
    }

    @Override
    public long bulkJsonUpdate(List<String> ids, List<String> jsons) {
        return bulkJsonUpdate(ids, jsons, true);
    }

    @Override
    public long bulkJsonUpdate(List<String> ids, List<String> jsons, boolean rebuidIndex) {
        if (null == ids || null == jsons || ids.size() != jsons.size())
            throw new SearchRuntimeException("bulk json update Null parameters or size not equal!");
        BulkRequest bulkRequest = new BulkRequest();
        UpdateRequest request = null;
        int i = 0;
        for (String documentId : ids) {
            request = new UpdateRequest(indexName, documentId).doc(jsons.get(i), XContentType.JSON)
                    .upsert(jsons.get(i++), XContentType.JSON);
            bulkRequest.add(request);
        }
        return bulkUpdate(bulkRequest);
    }

    @Override
    public <T> long bulkUpdate(List<String> ids, List<T> datas) {
        return bulkUpdate(ids, datas, true);
    }

    @Override
    public <T> long bulkUpdate(List<String> ids, List<T> datas, boolean rebuidIndex) {
        if (null == ids || null == datas || ids.size() != datas.size())
            throw new SearchRuntimeException("Null parameters or size not equal!");
        BulkRequest bulkRequest = new BulkRequest();
        UpdateRequest request = null;
        int i = 0;
        for (String documentId : ids) {
            request = new UpdateRequest(indexName, documentId).doc(esgson.toJson(datas.get(i)), XContentType.JSON)
                    .upsert(esgson.toJson(datas.get(i++)), XContentType.JSON);
            bulkRequest.add(request);
        }
        return bulkUpdate(bulkRequest);
    }

    @Override
    public long bulkUpdate(List<String> ids, Set<JsonBuilder> jsonBuilders) {
        return bulkUpdate(ids, jsonBuilders, true);
    }

    @Override
    public long bulkUpdate(List<String> ids, Set<JsonBuilder> jsonBuilders, boolean rebuidIndex) {
        if (null == ids || null == jsonBuilders || ids.size() != jsonBuilders.size())
            throw new SearchRuntimeException("Null parameters or size not equal!");
        BulkRequest bulkRequest = new BulkRequest();
        UpdateRequest request = null;
        int i = 0;
        for (JsonBuilder jsonBuilder : jsonBuilders) {
            request = new UpdateRequest(indexName, ids.get(i++)).doc(jsonBuilder.getBuilder())
                    .upsert(jsonBuilder.getBuilder());
            bulkRequest.add(request);
        }
        return bulkUpdate(bulkRequest);
    }

    @Override
    public long bulkMapUpsert(List<String> ids, List<Map<String, Object>> datas) {
        return bulkMapUpdate(ids, datas);
    }

    @Override
    public long bulkMapUpsert(List<String> ids, List<Map<String, Object>> datas, boolean rebuidIndex) {
        return bulkMapUpdate(ids, datas, rebuidIndex);
    }

    @Override
    public long bulkJsonUpsert(List<String> ids, List<String> jsons) {
        return bulkJsonUpdate(ids, jsons);
    }

    @Override
    public long bulkJsonUpsert(List<String> ids, List<String> jsons, boolean rebuidIndex) {
        return bulkJsonUpdate(ids, jsons, rebuidIndex);
    }

    @Override
    public <T> long bulkUpsert(List<String> ids, List<T> datas) {
        return bulkUpdate(ids, datas);
    }

    @Override
    public <T> long bulkUpsert(List<String> ids, List<T> datas, boolean rebuidIndex) {
        return bulkUpdate(ids, datas, rebuidIndex);
    }

    @Override
    public long bulkUpsert(List<String> ids, Set<JsonBuilder> jsonBuilders) {
        return bulkUpsert(ids, jsonBuilders);
    }

    @Override
    public long bulkUpsert(List<String> ids, Set<JsonBuilder> jsonBuilders, boolean rebuidIndex) {
        return bulkUpsert(ids, jsonBuilders, rebuidIndex);
    }

    private <T> Result<T> search(QueryBuilder queryBuilder, int from, int offset, List<Sort> sorts, Class<T> clazz,
            List<SearchCriteria> searchCriterias, String[] resultFields) {
        return search(queryBuilder, from, offset, sorts, clazz, null, searchCriterias, resultFields);
    }

    @Override
    public <T> Result<T> search(QueryBuilder queryBuilder, int from, int offset, @Nullable List<Sort> sorts,
            Class<T> clazz) {
        return search(queryBuilder, from, offset, sorts, clazz, null, null, null);
    }

    @Override
    public <T> Result<T> search(QueryBuilder queryBuilder, @Nullable List<Sort> sorts, Class<T> clazz) {
        return search(queryBuilder, 0, 1000, sorts, clazz, null, null, null);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <T> Result<T> search(QueryBuilder queryBuilder, int from, int offset, @Nullable List<Sort> sorts,
            TypeGetter typeGetter) {
        return search(queryBuilder, from, offset, sorts, null, typeGetter, null, null);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public <T> Result<T> search(QueryBuilder queryBuilder, @Nullable List<Sort> sorts, TypeGetter typeGetter) {
        return search(queryBuilder, 0, 1000, sorts, null, typeGetter, null, null);
    }

    @Override
    public <T extends GeoLocation> Result<T> geoDistanceQuery(List<SearchCriteria> searchCriterias, int from,
            int offset, Class<T> clazz, GeoLocation geoWhere) {
        // 创建query
        QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
        return search(queryBuilder, from, offset, clazz, null, null, null, geoWhere, DistanceUnit.KILOMETERS);
    }

    @Override
    public <T extends GeoLocation> Result<T> geoDistanceQuery(QueryBuilder queryBuilder, int from, int offset,
            Class<T> clazz, GeoLocation geoWhere, DistanceUnit unit) {
        return search(queryBuilder, from, offset, clazz, null, null, null, geoWhere, unit);
    }

    @Override
    public <T extends GeoLocation> Result<T> geoDistanceQuery(List<SearchCriteria> searchCriterias, int from,
            int offset, Class<T> clazz, GeoLocation geoWhere, DistanceUnit unit) {
        // 创建query
        QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
        return search(queryBuilder, from, offset, clazz, null, null, null, geoWhere, unit);
    }

    @Override
    public <T extends GeoLocation> Result<T> geoDistanceQuery(QueryBuilder queryBuilder, int from, int offset,
            Class<T> clazz, GeoLocation geoWhere) {
        return search(queryBuilder, from, offset, clazz, null, null, null, geoWhere, DistanceUnit.KILOMETERS);
    }

    private <T extends GeoLocation> Result<T> search(QueryBuilder queryBuilder, int from, int offset, Class<T> clazz,
            @SuppressWarnings("rawtypes") TypeGetter typeGetter, List<SearchCriteria> searchCriterias,
            String[] resultFields, GeoLocation geoWhere, DistanceUnit unit) {
        Result<T> result = new Result<>();
        result.setResultCode(Constant.ExceptionCode.SYSTEM_ERROR);
        try {
            /* 查询搜索总数 */
            // 此种实现不好，查询两次。即使分页，也可以得到总数
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            // 组装位置条件
            QueryBuilder geoDistanceQueryBuilder = QueryBuilders.geoDistanceQuery(geoWhere.getGeoField())
                    .point(geoWhere.getLocation().getLat(), geoWhere.getLocation().getLon())
                    .distance(geoWhere.getDistance(), unit);
            QueryBuilder finalQuery = QueryBuilders.boolQuery().must(queryBuilder).filter(geoDistanceQueryBuilder);
            searchSourceBuilder.timeout(new TimeValue(60000)).size(100).query(finalQuery);
            // 增加高亮
            if (null != searchCriterias) {
                searchSourceBuilder = searchHelper.createHighlight(searchSourceBuilder, searchCriterias, highlightCSS);
            }
            // 增加排序
            searchSourceBuilder.sort(SortBuilders.geoDistanceSort(geoWhere.getGeoField(),
                    geoWhere.getLocation().getLat(), geoWhere.getLocation().getLon()).order(SortOrder.ASC).unit(unit));

            logger.info("--ES search:\r\n{}", Strings.toString(searchSourceBuilder));

            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            searchRequest.scroll(scroll);
            if (null != resultFields && resultFields.length > 0)
                searchSourceBuilder.fetchSource(resultFields, new String[] {});
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            List<T> list = searchHelper.getGeoResult(client, scroll, searchResponse, clazz, typeGetter, from, offset);

            result.setContents(list);
            result.setCounts(searchResponse.getHits().getTotalHits().value);
            result.setResultCode(Constant.RPC_CALL_OK);
        } catch (Exception e) {
            throw new SearchRuntimeException("ES searchIndex error!", e);
        }
        return result;
    }

    private <T> Result<T> search(QueryBuilder queryBuilder, int from, int offset, List<Sort> sorts, Class<T> clazz,
            @SuppressWarnings("rawtypes") TypeGetter typeGetter, List<SearchCriteria> searchCriterias,
            String[] resultFields) {
        Result<T> result = new Result<>();
        result.setResultCode(Constant.ExceptionCode.SYSTEM_ERROR);
        try {
            /* 查询搜索总数 */
            // 此种实现不好，查询两次。即使分页，也可以得到总数
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.timeout(new TimeValue(60000)).size(100).query(queryBuilder);
            if (sorts == null || sorts.isEmpty()) {
                /* 如果不需要排序 */
            } else {
                /* 如果需要排序 */
                for (Sort sort : sorts) {
                    SortOrder sortOrder = sort.getOrder().compareTo(Sort.SortOrder.DESC) == 0 ? SortOrder.DESC
                            : SortOrder.ASC;

                    searchSourceBuilder.sort(sort.getSortBy(), sortOrder);
                }
            }
            // 增加高亮
            if (null != searchCriterias) {
                searchSourceBuilder = searchHelper.createHighlight(searchSourceBuilder, searchCriterias, highlightCSS);
            }
            logger.info("--ES search:\r\n{}", Strings.toString(searchSourceBuilder));
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            searchRequest.scroll(scroll);
            if (null != resultFields && resultFields.length > 0)
                searchSourceBuilder.fetchSource(resultFields, new String[] {});
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            List<T> list = searchHelper.getSearchResult(client, scroll, searchResponse, clazz, typeGetter, from,
                    offset);

            result.setContents(list);
            result.setCounts(searchResponse.getHits().getTotalHits().value);
            result.setResultCode(Constant.RPC_CALL_OK);
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
        result.setResultCode(Constant.ExceptionCode.SYSTEM_ERROR);
        try {
            QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
            if (queryBuilder == null)
                return result;
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.timeout(new TimeValue(60000)).size(100).query(queryBuilder);
            searchSourceBuilder.aggregation(AggregationBuilders.terms(field + "_aggs").field(field).size(100));
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            Terms sortAggregate = searchResponse.getAggregations().get(field + "_aggs");
            for (Terms.Bucket entry : sortAggregate.getBuckets()) {
                result.addAgg(new AggResult(entry.getKeyAsString(), entry.getDocCount(), field));
            }
            result.setCounts(searchResponse.getHits().getTotalHits().value);
            result.setResultCode(Constant.RPC_CALL_OK);
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
        result.setResultCode(Constant.ExceptionCode.SYSTEM_ERROR);
        try {
            QueryBuilder queryBuilder = searchHelper.createQueryBuilder(searchCriterias);
            if (queryBuilder == null)
                return result;
            result.setResultCode(Constant.RPC_CALL_OK);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.timeout(new TimeValue(60000)).size(100).query(queryBuilder);

            for (AggField aggField : fields) {
                // 先建第一级
                TermsAggregationBuilder termBuilder = AggregationBuilders.terms(aggField.getField() + "_aggs")
                        .field(aggField.getField()).size(100);
                // 循环创建子聚合
                termBuilder = searchHelper.addSubAggs(termBuilder, aggField.getSubAggs());
                searchSourceBuilder.aggregation(termBuilder);
            }
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            logger.info("agg search:\r\n{}", Strings.toString(searchSourceBuilder));

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            result.setCounts(searchResponse.getHits().getTotalHits().value);
            result.setAggs(searchHelper.getAgg(searchResponse, fields));
        } catch (Exception e) {
            throw new SearchRuntimeException("aggregation error", e);
        }
        return result;
    }

    @Override
    public <T> Result<T> fullTextSearch(String field, String text, int from, int offset, List<Sort> sorts,
            Class<T> clazz) {
        Result<T> result = new Result<>();
        result.setResultCode(Constant.ExceptionCode.SYSTEM_ERROR);
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.timeout(new TimeValue(60000)).size(100)
                    .query(QueryBuilders.matchQuery(field, text).operator(Operator.AND).minimumShouldMatch("75%"));
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            searchRequest.scroll(scroll);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

            List<T> list = searchHelper.getSearchResult(client, scroll, response, clazz, null, from, offset);

            result.setContents(list);
            result.setCounts(response.getHits().getTotalHits().value);

            result.setResultCode(Constant.RPC_CALL_OK);
        } catch (Exception e) {
            throw new SearchRuntimeException("ES searchIndex error", e);
        }
        return result;
    }

    @Override
    public <T> Result<T> fullTextSearch(String text, List<String> qryFields, List<AggField> aggFields, int from,
            int offset, List<Sort> sorts, Class<T> clazz) {
        Result<T> result = new Result<>();
        result.setResultCode(Constant.ExceptionCode.SYSTEM_ERROR);
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            // 如果带聚合必须指定对哪些字段
            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
            for (String qryField : qryFields) {
                queryBuilder.should(
                        QueryBuilders.matchQuery(qryField, text).operator(Operator.AND).minimumShouldMatch("75%"));
            }
            searchSourceBuilder.timeout(new TimeValue(60000)).size(100).query(queryBuilder);

            if (sorts == null || sorts.isEmpty()) {
                /* 如果不需要排序 */
            } else {
                /* 如果需要排序 */
                for (Sort sort : sorts) {
                    SortOrder sortOrder = sort.getOrder().compareTo(Sort.SortOrder.DESC) == 0 ? SortOrder.DESC
                            : SortOrder.ASC;

                    searchSourceBuilder.sort(sort.getSortBy(), sortOrder);
                }
            }
            // 此处加上聚合内容
            for (AggField aggField : aggFields) {
                // 先建第一级
                TermsAggregationBuilder termBuilder = AggregationBuilders.terms(aggField.getField() + "_aggs")
                        .field(aggField.getField()).size(100);
                // 循环创建子聚合
                termBuilder = searchHelper.addSubAggs(termBuilder, aggField.getSubAggs());
                searchSourceBuilder.aggregation(termBuilder);
            }
            SearchRequest searchRequest = new SearchRequest(indexName);
            searchRequest.source(searchSourceBuilder);
            logger.info("full text search:\r\n{}", Strings.toString(searchSourceBuilder));
            Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            searchRequest.scroll(scroll);
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            List<T> list = searchHelper.getSearchResult(client, scroll, response, clazz, null, from, offset);
            result.setContents(list);
            result.setCounts(response.getHits().getTotalHits().value);
            result.setAggs(searchHelper.getAgg(response, aggFields));
            result.setResultCode(Constant.RPC_CALL_OK);
        } catch (Exception e) {
            throw new SearchRuntimeException("ES searchIndex error", e);
        }
        return result;
    }

    @Override
    public <T> T getById(String id, Class<T> clazz) {
        GetRequest request = new GetRequest(indexName, indexName).id(id);
        GetResponse response = null;
        try {
            response = client.get(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new SearchRuntimeException("ES get error!{}", id, e);
        }
        if (null != response && response.isExists()) {
            return esgson.fromJson(response.getSourceAsString(), clazz);
        } else
            return null;
    }

    @Override
    public String getById(String id) {
        GetRequest request = new GetRequest(indexName, indexName).id(id);
        GetResponse response = null;
        try {
            response = client.get(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new SearchRuntimeException("ES get error!{}", id, e);
        }
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
        CreateIndexRequest request = new CreateIndexRequest(indexName);// 创建索引
        // 创建的每个索引都可以有与之关联的特定设置。
        String setting = " {" + "\"number_of_shards\":\"" + shards + "\"," + "\"number_of_replicas\":\"" + replicas
                + "\"," + " \"analysis\": {" + "         \"filter\": {" + "            \"nGram_filter\": {"
                + "               \"type\": \"nGram\"," + "               \"min_gram\": 1,"
                + "               \"max_gram\": 2" + "            }" + "         }," + "         \"analyzer\": {"
                + "            \"nGram_analyzer\": {" + "               \"type\": \"custom\","
                + "               \"tokenizer\": \"ik_max_word\"," + "               \"filter\": ["
                + "                  \"stop\"," + "                  \"nGram_filter\"" + "               ]"
                + "            }" + "         }" + "      }" + "   " + "}";
        request.settings(setting, XContentType.JSON);
        CreateIndexResponse createResponse = null;
        try {
            createResponse = client.indices().create(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new SearchRuntimeException("ES create error!", e);
        }
        return createResponse.isAcknowledged();
    }

    @Override
    public boolean deleteIndex(String indexName) {
        AcknowledgedResponse delete;
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            delete = client.indices().delete(request, RequestOptions.DEFAULT);
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
    public boolean existIndex(String indexName) {
        Assert.notNull(indexName, "Index Name can not be null");
        try {
            return client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
        } catch (Exception e) {
            throw new SearchRuntimeException("ES exist {} index error", indexName, e);
        }
    }

    @Override
    public boolean refresh() {
        RefreshResponse response;
        try {
            response = client.indices().refresh(new RefreshRequest(indexName), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new SearchRuntimeException("ES refresh index error", e);
        }
        return null != response && response.getFailedShards() <= 0;
    }

    @Override
    public <T> Result<T> fullTextSearch(String field, String text, List<AggField> aggFields, int from, int offset,
            List<Sort> sorts, Class<T> clazz) {
        List<String> qryFields = new ArrayList<>();
        qryFields.add(field);
        return fullTextSearch(text, qryFields, aggFields, from, offset, sorts, clazz);
    }

    @Override
    public void close() {
        if (null != client) {
            try {
                client.close();
            } catch (IOException e) {
                throw new SearchRuntimeException("ES client close error", e);
            }
            client = null;
        }
    }

    @Override
    public boolean createIndex(String indexName, String settings) {
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        request.settings(settings, XContentType.JSON);
        CreateIndexResponse createResponse = null;
        try {
            createResponse = client.indices().create(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new SearchRuntimeException("ES create error!", e);
        }
        return createResponse.isAcknowledged();
    }

    @Override
    public boolean existMapping(String indexName, String mapping) {
        GetMappingsRequest request = new GetMappingsRequest();
        request.indices(indexName);
        GetMappingsResponse response;
        try {
            response = client.indices().getMapping(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new SearchRuntimeException("ES get mapping error!", e);
        }
        if (null != response) {
            return response.mappings().containsKey(mapping);
        }
        return false;
    }

    @Override
    public boolean addMapping(String indexName, String json, boolean addDynamicTemplate) {
        // 这里要做些处理，如果用户没有type,或者对应不上应该报错
        Assert.notNull(indexName, "Index Name can not input null");
        Assert.notNull(json, "mapping define can not null");
        // 转换成json看看
        JsonObject typeObj = null;
        JsonObject jsonObj = esgson.fromJson(json, JsonObject.class);
        if (null == jsonObj.get("properties")) {
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
            typeObj = properties;
            // 这里也好办了,补上一层
        } else {
            // 存在就看自己是否正确构造
            typeObj = jsonObj;
        }
        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        putMappingRequest.source(esgson.toJson(typeObj), XContentType.JSON);
        logger.info("mapping info:\r\n{}", esgson.toJson(typeObj));
        AcknowledgedResponse putMappingResponse = null;
        try {
            putMappingResponse = client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new SearchRuntimeException("put mapping error!", e);
        }
        return null != putMappingResponse && putMappingResponse.isAcknowledged();
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
    public boolean addMapping(String indexName, String json, List<DynamicMatchOption> matchs) {
        // 这里要做些处理，如果用户没有type,或者对应不上应该报错
        Assert.notNull(indexName, "Index Name can not null");
        Assert.notNull(json, "mapping can not null");
        // 转换成json看看
        JsonObject typeObj = null;
        JsonObject jsonObj = esgson.fromJson(json, JsonObject.class);
        if (null == jsonObj.get("properties")) {
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
            typeObj = properties;
            // 这里也好办了,补上一层
        } else {
            // 存在就看自己是否正确构造
            typeObj = jsonObj;
        }
        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        putMappingRequest.source(esgson.toJson(typeObj), XContentType.JSON);
        AcknowledgedResponse putMappingResponse = null;
        try {
            putMappingResponse = client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new SearchRuntimeException("ES put mapping error!", e);
        }
        return null != putMappingResponse && putMappingResponse.isAcknowledged();
    }

    @Override
    public boolean setRefeshTime(long seconds) {
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexName);
        updateSettingsRequest.settings(Settings.builder().put("index.refresh_interval", seconds + "s").build());

        AcknowledgedResponse usrp;
        try {
            usrp = client.indices().putSettings(updateSettingsRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new SearchRuntimeException("ES put setting error!", e);
        }
        return null != usrp && usrp.isAcknowledged();
    }

    @Override
    public boolean addMapping(String indexName, String json) {
        // 这里要做些处理，如果用户没有type,或者对应不上应该报错
        Assert.notNull(indexName, "Index Name can not null");
        Assert.notNull(json, "mapping can not null");
        PutMappingRequest putMappingRequest = new PutMappingRequest(indexName);
        putMappingRequest.source(json, XContentType.JSON);
        AcknowledgedResponse putMappingResponse = null;
        try {
            putMappingResponse = client.indices().putMapping(putMappingRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new SearchRuntimeException("ES put mapping error!", e);
        }
        return null != putMappingResponse && putMappingResponse.isAcknowledged();
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
            SearchRequest request = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.timeout(new TimeValue(60000)).size(0).query(queryBuilder);

            ValueCountAggregationBuilder vcb = AggregationBuilders.count("agg").field(field).missing(0);
            searchSourceBuilder.aggregation(vcb);
            request.source(searchSourceBuilder);
            logger.info("--ES count search:\r\n{}", Strings.toString(searchSourceBuilder));
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

            if (null != searchResponse.getAggregations()) {
                ValueCount agg = searchResponse.getAggregations().get("agg");
                long value = agg.getValue();
                result.setCount(value);
            }
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
            SearchRequest request = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.timeout(new TimeValue(60000)).size(0).query(queryBuilder);
            StatsAggregationBuilder sb = AggregationBuilders.stats("agg").field(field).missing(0);
            searchSourceBuilder.aggregation(sb);
            logger.info("--ES stat search:\r\n{}", Strings.toString(searchSourceBuilder));
            request.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);

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
            throw new SearchRuntimeException("ES simple Aggregation error", e);
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
            SearchRequest request = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.timeout(new TimeValue(60000)).size(0).query(queryBuilder);

            TermsAggregationBuilder tb = AggregationBuilders.terms(groupAggName).field(groupBy)
                    .subAggregation(AggregationBuilders.stats("agg").field(field).missing(0));
            searchSourceBuilder.aggregation(tb);
            logger.info("--ES stat search:\r\n{}", Strings.toString(searchSourceBuilder));
            request.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            // 得到所有桶
            Terms aggs = searchResponse.getAggregations().get(groupAggName);
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
            SearchRequest request = new SearchRequest(indexName);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.timeout(new TimeValue(60000)).size(0).query(queryBuilder);

            TermsAggregationBuilder tb = AggregationBuilders.terms(groupAggName).field(groupBy)
                    .subAggregation(AggregationBuilders.count("agg").field(field).missing(0));
            searchSourceBuilder.aggregation(tb);
            logger.info("--ES count search:\r\n{}", Strings.toString(searchSourceBuilder));
            request.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(request, RequestOptions.DEFAULT);
            // 得到所有桶
            Terms aggs = searchResponse.getAggregations().get(groupAggName);
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

    @Override
    public boolean addMapping(String indexName, String json, String id) {
        this.id = id;
        return addMapping(indexName, json);
    }

}
