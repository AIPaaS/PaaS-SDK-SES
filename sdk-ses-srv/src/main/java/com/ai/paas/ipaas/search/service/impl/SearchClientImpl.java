package com.ai.paas.ipaas.search.service.impl;

//搜索实现定义

import com.ai.paas.ipaas.search.constants.SearchClientException;
import com.ai.paas.ipaas.search.service.ISearchClient;
import com.ai.paas.ipaas.search.vo.*;
import com.ai.paas.ipaas.search.vo.SearchOption.SearchLogic;
import com.ai.paas.ipaas.util.StringUtil;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.suggest.SuggestResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion.Entry.Option;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;


public class SearchClientImpl implements ISearchClient {
    private Logger logger = LoggerFactory.getLogger(SearchClientImpl.class);
    //  private Client searchClient = null;
    private String highlightCSS = "span,span";
    private String indexName;
    private JsonObject mapping;
    private String _id;

    //创建私有对象
    private TransportClient searchClient;

    public SearchClientImpl(String hosts, String indexName,
                            JsonObject mapping, JsonObject idObj) {
        this.indexName = indexName;
        this.mapping = mapping;
        _id = idObj.get("path").toString().replaceAll("\"", "");
        List<String> clusterList = new ArrayList<String>();
        try {
            Class<?> clazz = Class.forName(TransportClient.class.getName());
            Constructor<?> constructor = clazz
                    .getDeclaredConstructor(Settings.class);
            constructor.setAccessible(true);
            searchClient = (TransportClient) constructor.newInstance(settings);
            if (!StringUtil.isBlank(hosts)) {
                clusterList = Arrays.asList(hosts.split(","));
            }
            for (String item : clusterList) {
                String address = item.split(":")[0];
                int port = Integer.parseInt(item.split(":")[1]);
                /* 通过tcp连接搜索服务器，如果连接不上，有一种可能是服务器端与客户端的jar包版本不匹配 */
                searchClient
                        .addTransportAddress(new InetSocketTransportAddress(
                                address, port));
            }
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("ES init client error", e);
        }

    }

    static Settings settings = ImmutableSettings.settingsBuilder()
//        .put(this.searchClientConfigureMap)
            .put("client.transport.ping_timeout", "10s")
            .put("client.transport.sniff", "true")
            .put("client.transport.ignore_cluster_name", "true")
            .build();


    //取得实例
    public synchronized TransportClient getTransportClient() {
        return searchClient;
    }

    public void setHighlightCSS(String highlightCSS) {
        this.highlightCSS = highlightCSS;
    }


    private boolean _bulkInsertData(String indexName, XContentBuilder xContentBuilder) {
        try {
            BulkRequestBuilder bulkRequest = searchClient.prepareBulk();
            bulkRequest.add(searchClient.prepareIndex(indexName, indexName).setSource(xContentBuilder));
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (!bulkResponse.hasFailures()) {
                return true;
            } else {
                this.logger.error("FailureMessage", bulkResponse.buildFailureMessage());
                throw new SearchClientException("ES insert error", bulkResponse.buildFailureMessage());
            }
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("ES insert error", e);
        }
    }


    @SuppressWarnings("deprecation")
    public boolean deleteData(List<SearchfieldVo> fieldList) {
        try {
            QueryBuilder queryBuilder = null;
            queryBuilder = this.createQueryBuilder(fieldList, SearchLogic.must);
            this.logger.warn("[" + indexName + "]" + queryBuilder.toString());
            searchClient.prepareDeleteByQuery(indexName).setQuery(queryBuilder).execute().actionGet();
            return true;
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("ES delete error", e);
        }
    }

    @Override
    public boolean deleteData(String index, String type, String id) {
        try {
            searchClient.prepareDelete(index, type, id).execute().actionGet();
            return true;
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("ES delete error", e);
        }
    }


    @SuppressWarnings("deprecation")
    public boolean cleanData() {
        try {
            QueryBuilder queryBuilder = QueryBuilders.boolQuery();
            this.logger.warn("[" + indexName + "]" + queryBuilder.toString());
            searchClient.prepareDeleteByQuery(indexName).setQuery(queryBuilder).execute().actionGet();
            return true;
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("ES delete error", e);
        }
    }


    @SuppressWarnings("unchecked")
    public boolean insertData(String jsonData) {

        if (jsonData == null || "".equals(jsonData)) {
            throw new SearchClientException("插入数据参数为空");
        }
        XContentBuilder xContentBuilder = null;
        try {
            xContentBuilder = XContentFactory.jsonBuilder().startObject();
        } catch (IOException e) {
            this.logger.error(e.getMessage());
            throw new SearchClientException("插入数据异常", e);
        }

        Map<String, Object> dataMap = new HashMap<String, Object>();
        Gson gson = new Gson();
        dataMap = gson.fromJson(jsonData, dataMap.getClass());
        Iterator<Entry<String, Object>> iterator = dataMap.entrySet().iterator();
        boolean flag = false;
        while (iterator.hasNext()) {
            Entry<String, Object> entry = iterator.next();
            String field = entry.getKey().toLowerCase();
            Object values = entry.getValue();
            if (_id.equals(field)) {
                flag = true;
            }
            //判断该字段是否是推荐类型字段   type为 completion

            JsonElement fieldElement = mapping.get(field);
            if (fieldElement == null) {
                throw new SearchClientException("the field is not exist in index：" + field, "the field is not exist in index：" + field);
            }
            JsonObject fieldObject = fieldElement.getAsJsonObject();

            if ("string".equals(fieldObject.get("type").getAsString())) {
                values = String.valueOf(values);
            }
            if ("integer".equals(fieldObject.get("type").getAsString())) {
                values = Double.valueOf(String.valueOf(values)).intValue();
            }
            if ("completion".equals(fieldObject.get("type").getAsString())) {
                Map<String, Object> map = new HashMap<String, Object>();
                Map<String, Object> inputmap = new HashMap<String, Object>();
                inputmap.put("input", values);
                map.put(field, inputmap);
                values = inputmap;
            }
            try {
                xContentBuilder = xContentBuilder.field(field, values);
            } catch (IOException e) {
                this.logger.error(e.getMessage(), e);
                throw new SearchClientException("插入数据异常", e);
            }
        }
        if (!flag) {
            this.logger.error("the unique key is null", "the unique key is null" + _id);
            throw new SearchClientException("the unique key is null,please check your data", "the unique key is null" + _id);
        }
        try {
            xContentBuilder = xContentBuilder.endObject();
        } catch (IOException e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("插入数据异常", e);
        }
        try {
            xContentBuilder.string();
        } catch (IOException e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("插入数据异常", e);
        }
        return this._bulkInsertData(indexName, xContentBuilder);
    }


    @SuppressWarnings("unchecked")
    public boolean bulkInsertData(List<String> datalist) {
        BulkRequestBuilder bulkRequest = searchClient.prepareBulk();
        for (String data : datalist) {
            XContentBuilder xContentBuilder = null;
            try {
                xContentBuilder = XContentFactory.jsonBuilder().startObject();
            } catch (IOException e) {
                this.logger.error(e.getMessage(), e);
                throw new SearchClientException("插入数据异常", e);
            }

            Map<String, Object> dataMap = new HashMap<String, Object>();
            Gson gson = new Gson();
            dataMap = gson.fromJson(data, dataMap.getClass());
            Iterator<Entry<String, Object>> iterator = dataMap.entrySet().iterator();
            boolean flag = false;
            while (iterator.hasNext()) {
                Entry<String, Object> entry = iterator.next();
                String field = entry.getKey().toLowerCase();
                Object values = entry.getValue();
                if (_id.equals(field)) {
                    flag = true;
                }
                //判断该字段是否是推荐类型字段   type为 completion
                JsonElement fieldElement = mapping.get(field);
                if (fieldElement == null) {
                    throw new SearchClientException("the field is not exist in index：" + field, "the field is not exist in index：" + field);
                }
                JsonObject fieldObject = fieldElement.getAsJsonObject();
                if (fieldObject.get("type") != null) {
                    if ("string".equals(fieldObject.get("type").getAsString())) {
                        values = String.valueOf(values);
                    }
                    if ("integer".equals(fieldObject.get("type").getAsString())) {
                        values = Double.valueOf(String.valueOf(values)).intValue();
                    }

                    if ("object".equals(fieldObject.get("type").getAsString())) {
                        values = gson.toJson(values);
                    }
                    if ("completion".equals(fieldObject.get("type").getAsString())) {
                        Map<String, Object> map = new HashMap<String, Object>();
                        Map<String, Object> inputmap = new HashMap<String, Object>();
                        inputmap.put("input", values);
                        map.put(field, inputmap);
                        values = inputmap;
                    }
                    try {
                        xContentBuilder = xContentBuilder.field(field, values);
                    } catch (IOException e) {
                        this.logger.error(e.getMessage(), e);
                        throw new SearchClientException("插入数据异常", e);
                    }
                } else {
                    if (values instanceof Map) {

                        String val = gson.toJson(values);
                        Map<String, Object> map = new HashMap<String, Object>();

                        map = gson.fromJson(val, map.getClass());
                        try {
                            xContentBuilder = xContentBuilder.field(field, map);
                        } catch (IOException e) {
                            this.logger.error(e.getMessage(), e);
                            throw new SearchClientException("插入数据异常", e);
                        }
                    } else if (values instanceof List) {
                        String val = gson.toJson(values);
                        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();

                        list = gson.fromJson(val, list.getClass());
                        try {
                            xContentBuilder = xContentBuilder.field(field, list);
                        } catch (IOException e) {
                            this.logger.error(e.getMessage(), e);
                            throw new SearchClientException("插入数据异常", e);
                        }
                    }
                }
            }
            if (!flag) {
                this.logger.error("the unique key is null", "the unique key is null" + _id);
                throw new SearchClientException("the unique key is null,please check your data", "the unique key is null" + _id);
            }
            try {
                xContentBuilder = xContentBuilder.endObject();
            } catch (IOException e) {
                this.logger.error(e.getMessage(), e);
                throw new SearchClientException("插入数据异常", e);
            }
            bulkRequest.add(searchClient.prepareIndex(indexName, indexName).setSource(xContentBuilder));
//		  this.insertData(data);
        }
        try {
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (!bulkResponse.hasFailures()) {
                return true;
            } else {
                this.logger.error("insert error", bulkResponse.buildFailureMessage());
                throw new SearchClientException("insert error", "insert error" + bulkResponse.buildFailureMessage());
            }
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("ES insert error", e);
        }
    }

    public boolean insertData(List<InsertFieldVo> fieldList) {
        XContentBuilder xContentBuilder = null;
        try {

            xContentBuilder = XContentFactory.jsonBuilder().startObject();
        } catch (IOException e) {
            this.logger.error(e.getMessage());
            return false;
        }
        for (InsertFieldVo vo : fieldList) {
            String field = vo.getFiledName();
            Object value = vo.getFiledValue();
            if (vo.getFileType().equals(InsertFieldVo.FiledType.completion)) {
                if (!(value instanceof HashMap)) {
                    this.logger.error("param error", "param error");
                    throw new SearchClientException("param error", "please check the completion param");
                }
            } else {
                if (value instanceof String) {
                    if (SearchOption.isDate(value))
                        value = SearchOption.formatDate(value);
                }
            }
            try {
                xContentBuilder = xContentBuilder.field(field, value);
            } catch (IOException e) {
                this.logger.error(e.getMessage(), e);

                return false;
            }
        }
        try {
            xContentBuilder = xContentBuilder.endObject();
        } catch (IOException e) {
            this.logger.error(e.getMessage(), e);
            return false;
        }

        return this._bulkInsertData(indexName, xContentBuilder);

    }


    public boolean updateData(List<SearchfieldVo> delFiledList, List<String> datalist) {
        return this.bulkInsertData(datalist);
    }


    public boolean updateData(List<String> datalist) {
        return this.bulkInsertData(datalist);
    }


    private RangeQueryBuilder createRangeQueryBuilder(String field, List<String> valuesSet) {
        String[] array = new String[2];
        String[] values = (String[]) valuesSet.toArray(array);
        if (values.length == 1 || values[1] == null || values[1].toString().trim().isEmpty()) {
            this.logger.warn("error", "[区间搜索]必须传递两个值，但是只传递了一个值，所以返回null");
            return null;
        }
        boolean timeType = false;
        if (SearchOption.isDate(values[0])) {
            if (SearchOption.isDate(values[1])) {
                timeType = true;
            }
        }
        String begin = "", end = "";
        if (timeType) {
          /*
           * 如果时间类型的区间搜索出现问题，有可能是数据类型导致的：
           *     （1）在监控页面（elasticsearch-head）中进行range搜索，看看什么结果，如果也搜索不出来，则：
           *     （2）请确定mapping中是date类型，格式化格式是yyyy-MM-dd HH:mm:ss
           *    （3）请确定索引里的值是类似2012-01-01 00:00:00的格式
           *    （4）如果是从数据库导出的数据，请确定数据库字段是char或者varchar类型，而不是date类型（此类型可能会有问题）
           * */
            begin = SearchOption.formatDate(values[0]);
            end = SearchOption.formatDate(values[1]);
        } else {
            begin = values[0].toString();
            end = values[1].toString();
        }
        return QueryBuilders.rangeQuery(field).from(begin).to(end);
    }

    /*
     * 创建过滤条件
     * */
    private QueryBuilder createSingleFieldQueryBuilder(String field, List<String> values, SearchOption mySearchOption) {
        try {
            if (mySearchOption.getSearchType() == SearchOption.SearchType.range) {
              /*区间搜索*/
                return this.createRangeQueryBuilder(field, values);
            }
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            if (values != null) {

                Iterator<String> iterator = values.iterator();
                while (iterator.hasNext()) {
                    QueryBuilder queryBuilder = null;
                    String formatValue = iterator.next().toString().trim().replace("*", "").toLowerCase();//格式化搜索数据
                    if (mySearchOption.getSearchType() == SearchOption.SearchType.term) {
                        queryBuilder = QueryBuilders.termQuery(field, formatValue).boost(mySearchOption.getBoost());
                    } else if (mySearchOption.getSearchType() == SearchOption.SearchType.querystring) {
                        if (formatValue.length() == 1) {
                          /*如果搜索长度为1的非数字的字符串，格式化为通配符搜索，暂时这样，以后有时间改成multifield搜索，就不需要通配符了*/
                            if (!Pattern.matches("[0-9]", formatValue)) {
                                formatValue = "*" + formatValue + "*";
                            }
                        }
                        @SuppressWarnings("deprecation")
                        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryString(formatValue).minimumShouldMatch(mySearchOption.getQueryStringPrecision());
                        queryBuilder = queryStringQueryBuilder.field(field).boost(mySearchOption.getBoost());
                    }
                    if (mySearchOption.getSearchLogic() == SearchLogic.should) {
                        boolQueryBuilder = boolQueryBuilder.should(queryBuilder);
                    } else {
                        boolQueryBuilder = boolQueryBuilder.must(queryBuilder);
                    }
                }
            }
            return boolQueryBuilder;
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("ES create builder error", e);
        }
    }

    /*
     * 创建搜索条件
     * */
    private QueryBuilder createQueryBuilder(List<SearchfieldVo> fieldList, SearchLogic searchLogic) {
        try {
            if (fieldList == null || fieldList.size() == 0) {
                return null;
            }
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            for (SearchfieldVo fieldVo : fieldList) {
                String field = fieldVo.getFiledName().toLowerCase();
                SearchOption mySearchOption = fieldVo.getOption();
                QueryBuilder queryBuilder = this.createSingleFieldQueryBuilder(field, fieldVo.getFiledValue(), mySearchOption);
                if (queryBuilder != null) {
                    if (searchLogic == SearchLogic.should) {
                      /*should关系，也就是说，在A索引里有或者在B索引里有都可以*/
                        boolQueryBuilder = boolQueryBuilder.should(queryBuilder);
                    } else {
                      /*must关系，也就是说，在A索引里有，在B索引里也必须有*/
                        boolQueryBuilder = boolQueryBuilder.must(queryBuilder);
                    }
                }

            }
            return boolQueryBuilder;
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("ES create builder error", e);

        }
    }


    private SearchResponse searchCountRequest(String indexNames, Object queryBuilder) {
        try {
            SearchRequestBuilder searchRequestBuilder = searchClient.prepareSearch(indexNames).setSearchType(SearchType.COUNT);
            if (queryBuilder instanceof QueryBuilder) {
                searchRequestBuilder = searchRequestBuilder.setQuery((QueryBuilder) queryBuilder);
            }
            if (queryBuilder instanceof byte[]) {
                String query = new String((byte[]) queryBuilder);
                searchRequestBuilder = searchRequestBuilder.setQuery(QueryBuilders.wrapperQuery(query));
            }
            return searchRequestBuilder.execute().actionGet();
        } catch (Exception e) {
            this.logger.error("search count error", e.getMessage(), e);
            throw new SearchClientException("ES search count error", e);
        }
    }


    /*获得搜索结果*/
    private List<Map<String, Object>> getSearchResult(SearchResponse searchResponse) {
        try {
            List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
            for (SearchHit searchHit : searchResponse.getHits()) {
                Iterator<Entry<String, Object>> iterator = searchHit.getSource().entrySet().iterator();
                HashMap<String, Object> resultMap = new HashMap<String, Object>();
                while (iterator.hasNext()) {
                    Entry<String, Object> entry = iterator.next();
                    resultMap.put(entry.getKey(), entry.getValue());
                }
                Map<String, HighlightField> highlightMap = searchHit.highlightFields();
                Iterator<Entry<String, HighlightField>> highlightIterator = highlightMap.entrySet().iterator();
                while (highlightIterator.hasNext()) {
                    Entry<String, HighlightField> entry = highlightIterator.next();
                    Object[] contents = entry.getValue().fragments();
                    if (contents.length == 1) {
                        resultMap.put(entry.getKey(), contents[0].toString());
                    } else {
                        this.logger.warn("搜索结果中的高亮结果出现多数据contents.length ", contents.length);
                    }
                }
                resultList.add(resultMap);
            }
            return resultList;
        } catch (Exception e) {
            this.logger.error("ES search error", e.getMessage());
            throw new SearchClientException("ES search error", e);
        }
    }


    /*获得搜索建议
     * 服务器端安装elasticsearch-plugin-suggest
     * 客户端加入elasticsearch-plugin-suggest的jar包
     * https://github.com/spinscale/elasticsearch-suggest-plugin
     * */
    public List<String> getSuggest(String fieldName, String value, int count) {
        try {
            CompletionSuggestionBuilder suggestionsBuilder = new CompletionSuggestionBuilder(
                    "complete");
            suggestionsBuilder.text(value);
            suggestionsBuilder.field(fieldName.toLowerCase());
            suggestionsBuilder.size(count);
            SuggestResponse resp = searchClient.prepareSuggest(indexName)
                    .addSuggestion(suggestionsBuilder).execute().actionGet();
            List<? extends org.elasticsearch.search.suggest.Suggest.Suggestion.Entry<? extends org.elasticsearch.search.suggest.Suggest.Suggestion.Entry.Option>> list =
                    resp.getSuggest().getSuggestion("complete").getEntries();
            List<String> returnList = new ArrayList<String>();
            for (int i = 0; i < list.size(); i++) {

                List<?> options = list.get(i).getOptions();
                returnList.add(list.get(i).getText().toString());
                for (int j = 0; j < options.size(); j++) {
                    if (options.get(j) instanceof Option) {
                        Option op = (Option) options.get(j);
                        returnList.add(op.getText().toString());

                    }

                }

            }

            return returnList;
        } catch (Exception e) {
            e.printStackTrace();
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("ES suggest error", e);
        }
    }

    @Override
    public Results<Map<String, Object>> search(List<SearchfieldVo> fieldList, int from, int offset, @Nullable String sortField, @Nullable String sortType) {
        Results<Map<String, Object>> result = new Results<Map<String, Object>>();
        result.setResultCode("999999");
        try {
            QueryBuilder queryBuild = this.generateBoolQueryBuilder(fieldList);
            if (queryBuild == null)
                return result;
            searchAction(fieldList, from, offset, sortField, sortType, result, queryBuild);
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("ES search error", e);
        }
        return result;
    }

    @Override
    public Results<Map<String, Long>> simpleAggregation(List<SearchfieldVo> fieldList, String sortFields) {
        Results<Map<String, Long>> result = new Results<Map<String, Long>>();
        result.setResultCode("999999");
        try {
            QueryBuilder queryBuild = this.generateBoolQueryBuilder(fieldList);
            if (queryBuild == null)
                return result;
            ;
            SearchResponse searchResponse = searchClient.prepareSearch(indexName).setSearchType(SearchType.DEFAULT)
                    .setQuery(queryBuild).addAggregation(AggregationBuilders.terms(sortFields + "_Aggregate")
                            .field(sortFields)).execute().get();

            Terms sortAggrate = searchResponse.getAggregations().get(sortFields + "_Aggregate");
            Map<String, Long> resultMap = new HashMap<String, Long>();
            for (Terms.Bucket entry : sortAggrate.getBuckets()) {
                resultMap.put(entry.getKey(), entry.getDocCount());
            }

            result.setCounts(resultMap.size());
            result.addSearchList(resultMap);
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("ES simpleAggregation error", e);
        }
        return result;
    }

    private void searchAction(List<SearchfieldVo> fieldList, int from, int offset, @Nullable String sortField, @Nullable String sortType, Results<Map<String, Object>> result, QueryBuilder queryBuild) {
        try {
          /* 查询搜索总数    */
            SearchResponse searchResponse = this.searchCountRequest(indexName, queryBuild);
            long count = searchResponse.getHits().totalHits();

            SearchRequestBuilder searchRequestBuilder = null;
            searchRequestBuilder = searchClient.prepareSearch(indexName).setSearchType(SearchType.DEFAULT)
                    .setQuery(queryBuild).setFrom(from).setSize(offset).setExplain(true);
            if (sortField == null || sortField.isEmpty() || sortType == null || sortType.isEmpty()) {
              /*如果不需要排序*/
            } else {
              /*如果需要排序*/
                org.elasticsearch.search.sort.SortOrder sortOrder = sortType.equals("desc") ? org.elasticsearch.search.sort.SortOrder.DESC : org.elasticsearch.search.sort.SortOrder.ASC;
                searchRequestBuilder = searchRequestBuilder.addSort(sortField.toLowerCase(), sortOrder);

            }
            searchRequestBuilder = this.createHighlight(searchRequestBuilder, fieldList);
            searchResponse = searchRequestBuilder.execute().actionGet();
            List<Map<String, Object>> list = this.getSearchResult(searchResponse);
            result.setSearchList(list);
            result.setCounts(count);
            result.setResultCode("000000");
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("ES searchIndex error", e);
        }
    }

    private QueryBuilder generateBoolQueryBuilder(List<SearchfieldVo> fieldList) {
        if (fieldList == null || fieldList.size() == 0)
            return null;

        BoolQueryBuilder rootQueryBuilder = QueryBuilders.boolQuery();
        for (SearchfieldVo fieldVo : fieldList) {
            if (fieldVo.hasSubSearchFieldVo()) {
                QueryBuilder tmpQueryBuilder = generateBoolQueryBuilder(fieldVo.getSubSearchFieldVo());
                if (tmpQueryBuilder != null) {
                    fieldVo.getOption().getSearchLogic().convertQueryBuilder(rootQueryBuilder, tmpQueryBuilder);
                }
            }

            fieldVo.getOption().getSearchLogic().convertQueryBuilder(rootQueryBuilder, createSingleFieldQueryBuilder
                    (fieldVo.getFormatFieldName(), fieldVo.getFiledValue(), fieldVo.getOption()));
        }

        return rootQueryBuilder;
    }


    public Results<Map<String, Object>> searchIndex(List<SearchfieldVo> fieldList,
                                                    int from, int offset, SearchLogic logic, @Nullable String sortField, @Nullable String sortType) {
        Results<Map<String, Object>> result = new Results<Map<String, Object>>();
        result.setResultCode("999999");
      /*创建must搜索条件*/
        QueryBuilder queryBuilder = this.createQueryBuilder(fieldList, logic);
        if (queryBuilder == null) {
            return result;
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder = boolQueryBuilder.must(queryBuilder);
        searchAction(fieldList, from, offset, sortField, sortType, result, boolQueryBuilder);
        return result;
    }


    public Results<Map<String, Object>> complexSearch(List<SearchVo> searchList,
                                                      int from, int offset, SearchLogic logic, @Nullable String sortField, @Nullable String sortType) {
        Results<Map<String, Object>> result = new Results<Map<String, Object>>();
        result.setResultCode("999999");
      /*创建must搜索条件*/
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        for (SearchVo searchVo : searchList) {
            List<SearchfieldVo> fieldList = searchVo.getSearchFieldList();
            SearchLogic searchLogic = searchVo.getSearchLogic();
            QueryBuilder queryBuilder = this.createQueryBuilder(fieldList, searchLogic);

            if (logic == SearchLogic.should) {
                boolQueryBuilder = boolQueryBuilder.should(queryBuilder);
            } else {
                boolQueryBuilder = boolQueryBuilder.must(queryBuilder);
            }

        }

        try {
          /* 查询搜索总数    */

            SearchRequestBuilder searchRequestBuilder = null;
            searchRequestBuilder = searchClient.prepareSearch(indexName).setSearchType(SearchType.DEFAULT)
                    .setQuery(boolQueryBuilder).setFrom(from).setSize(offset).setExplain(true);
            if (sortField == null || sortField.isEmpty() || sortType == null || sortType.isEmpty()) {
              /*如果不需要排序*/
            } else {
              /*如果需要排序*/
                org.elasticsearch.search.sort.SortOrder sortOrder = sortType.equals("desc") ? org.elasticsearch.search.sort.SortOrder.DESC : org.elasticsearch.search.sort.SortOrder.ASC;
                searchRequestBuilder = searchRequestBuilder.addSort(sortField.toLowerCase(), sortOrder);

            }
            for (SearchVo searchVo : searchList) {
                List<SearchfieldVo> fieldList = searchVo.getSearchFieldList();
                searchRequestBuilder = this.createHighlight(searchRequestBuilder, fieldList);
            }
            SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
            long count = searchResponse.getHits().totalHits();
            List<Map<String, Object>> list = this.getSearchResult(searchResponse);
            result.setSearchList(list);
            result.setCounts(count);
            result.setResultCode("000000");
        } catch (Exception e) {
            this.logger.error(e.getMessage(), e);
            throw new SearchClientException("ES searchIndex error", e);
        }
        return result;
    }

    /**
     * 创建高亮
     *
     * @param searchRequestBuilder
     * @param searchContentMap
     * @return
     */
    private SearchRequestBuilder createHighlight(SearchRequestBuilder searchRequestBuilder, List<SearchfieldVo> fieldList) {
        for (SearchfieldVo fieldVo : fieldList) {
            String field = fieldVo.getFiledName();
            SearchOption mySearchOption = fieldVo.getOption();
            if (mySearchOption.isHighlight()) {
              /*
               * http://www.elasticsearch.org/guide/reference/api/search/highlighting.html
               *
               * fragment_size设置成1000，默认值会造成返回的数据被截断
               * */
                searchRequestBuilder = searchRequestBuilder.addHighlightedField(field, 1000)
                        .setHighlighterPreTags("<" + this.highlightCSS.split(",")[0] + ">")
                        .setHighlighterPostTags("</" + this.highlightCSS.split(",")[1] + ">");

            }
        }

        return searchRequestBuilder;
    }


    public Results<Map<String, Object>> simpleSearch(byte[] query, int from, int offset, String sortField, String sortType) {
        if (offset <= 0) {
            return null;
        }
        Results<Map<String, Object>> result = new Results<Map<String, Object>>();
        result.setResultCode("999999");
        try {
          /* 查询搜索总数    */
            SearchRequestBuilder searchRequestBuilder = this.searchClient.prepareSearch(indexName).setSearchType(SearchType.DEFAULT)
                    .setExplain(true);


            if (sortField == null || sortField.isEmpty() || sortType == null || sortType.isEmpty()) {
              /*如果不需要排序*/
            } else {
              /*如果需要排序*/
                org.elasticsearch.search.sort.SortOrder sortOrder = sortType.equals("desc") ? org.elasticsearch.search.sort.SortOrder.DESC : org.elasticsearch.search.sort.SortOrder.ASC;
                searchRequestBuilder = searchRequestBuilder.addSort(sortField.toLowerCase(), sortOrder);
            }
            searchRequestBuilder.setFrom(from).setSize(offset);
            String queryStr = new String(query);
            searchRequestBuilder = searchRequestBuilder.setQuery(QueryBuilders.wrapperQuery(queryStr));
            this.logger.debug(queryStr);

            SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();
            long count = searchResponse.getHits().totalHits();
            List<Map<String, Object>> list = this.getSearchResult(searchResponse);
            result.setSearchList(list);
            result.setCounts(count);
            result.setResultCode("000000");
        } catch (Exception e) {
            this.logger.error(e.getMessage());
        }
        return null;
    }

}
