package com.ai.paas.ipaas.search.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.search.SearchRuntimeException;
import com.ai.paas.ipaas.search.common.TypeGetter;
import com.ai.paas.ipaas.search.vo.AggField;
import com.ai.paas.ipaas.search.vo.AggResult;
import com.ai.paas.ipaas.search.vo.GeoLocation;
import com.ai.paas.ipaas.search.vo.SearchCriteria;
import com.ai.paas.ipaas.search.vo.SearchOption;
import com.ai.paas.util.StringUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class SearchHelper {
    private Logger logger = LoggerFactory.getLogger(SearchHelper.class);

    private static final int MATCH_PHRASE_SLOP = 50;

    private String dtFmt = "yyyy-MM-dd'T'HH:mm:ssZZZ";

    private Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create();
    private Gson simpleGson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create();

    public BulkProcessor init(final RestHighLevelClient client, int batchSize) {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                logger.info("--- Bulk OP: try to action {} documents---", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                logger.info("---Bulk OP: bulk operation succeeded for {} documents---", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("---Bulk OP: bulk operation fail ---", failure);
            }
        };

        return BulkProcessor
                .builder((request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                        listener)
                .setBulkActions(batchSize).setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5)).setConcurrentRequests(5).build();
    }

    public void setDateFmt(String dateFmt) {
        gson = new GsonBuilder().setDateFormat(dateFmt).create();
        this.dtFmt = dateFmt;
    }

    public String getId(String json, String id) {
        JsonObject obj = gson.fromJson(json, JsonObject.class);
        if (null != obj.get(id))
            return obj.get(id).getAsString();
        else
            return null;
    }

    public String getId(XContentBuilder builder, String id) {
        String json;
        json = Strings.toString(builder);
        return getId(json, id);
    }

    public boolean hasId(XContentBuilder builder, String id) {
        String json;
        json = Strings.toString(builder);
        return hasId(json, id);
    }

    public boolean hasId(String json, String id) {

        JsonObject obj = gson.fromJson(json, JsonObject.class);
        return null != obj.get(id);
    }

    public QueryBuilder createStringSQLBuilder(String query) {
        try {
            if (StringUtil.isBlank(query)) {
                return null;
            }
            return QueryBuilders.queryStringQuery(query);
        } catch (Exception e) {
            throw new SearchRuntimeException("ES create query builder error", e);

        }
    }

    /**
     * 搜索条件，支持嵌套逻辑
     * 
     * @param searchCriterias
     * @return 索引对象
     */
    public QueryBuilder createQueryBuilder(List<SearchCriteria> searchCriterias) {
        try {
            if (searchCriterias == null || searchCriterias.isEmpty()) {
                return null;
            }
            BoolQueryBuilder rootQueryBuilder = QueryBuilders.boolQuery();
            for (SearchCriteria searchCriteria : searchCriterias) {
                if (searchCriteria.hasSubCriteria()) {
                    // 循环调用
                    QueryBuilder childQueryBuilder = createQueryBuilder(searchCriteria.getSubCriterias());
                    if (childQueryBuilder != null) {
                        searchCriteria.getOption().getSearchLogic().convertQueryBuilder(rootQueryBuilder,
                                childQueryBuilder);
                    }
                }

                QueryBuilder builder = searchCriteria.getBuilder();
                // 如果原生的查询不为空，则优先用原生的查询条件。gucl--20190219
                if (builder != null) {
                    searchCriteria.getOption().getSearchLogic().convertQueryBuilder(rootQueryBuilder, builder);
                }
                // 原生的查询条件为空时，按自定义的查询条件
                else {
                    String field = getLowerFormatField(searchCriteria.getField());
                    if (!StringUtil.isBlank(field)) {
                        SearchOption searchOption = searchCriteria.getOption();
                        QueryBuilder queryBuilder = createSingleFieldQueryBuilder(field, searchCriteria.getFieldValue(),
                                searchOption);
                        if (queryBuilder != null) {
                            searchCriteria.getOption().getSearchLogic().convertQueryBuilder(rootQueryBuilder,
                                    queryBuilder);
                        }
                    } // end if
                }

            }
            return rootQueryBuilder;
        } catch (Exception e) {
            throw new SearchRuntimeException("ES create builder error", e);

        }
    }

    private String getLowerFormatField(String field) {
        if (StringUtil.isBlank(field))
            return null;
        return field;
    }

    /*
     * 创建过滤条件
     */
    public QueryBuilder createSingleFieldQueryBuilder(String field, List<Object> values, SearchOption mySearchOption) {
        try {

            if (mySearchOption.getSearchType() == SearchOption.SearchType.range) {
                /* 区间搜索 */
                return createRangeQueryBuilder(field, values);
            }
            QueryBuilder queryBuilder = null;
            // 这里先加个是否存在判断
            if (mySearchOption.getDataFilter() == SearchOption.DataFilter.exists) {
                // 需要增加filter搜索
                queryBuilder = QueryBuilders.existsQuery(field);
                return queryBuilder;
            } else if (values != null) {
                String formatValue = null;
                List<String> terms = new ArrayList<>();
                Iterator<Object> iterator = values.iterator();
                Object obj = null;
                while (iterator.hasNext()) {
                    queryBuilder = null;
                    obj = iterator.next();
                    // 这里得判断是否为空
                    if (null == obj)
                        continue;
                    formatValue = obj.toString().trim().replaceAll("\\*", "");// 格式化搜索数据
                    formatValue = formatValue.replaceAll("\\^", "");// 格式化搜索数据
                    formatValue = QueryParser.escape(formatValue);
                    formatValue = formatValue.replaceAll("\\\\-", "-");
                    if (mySearchOption.getSearchType() == SearchOption.SearchType.querystring) {
                        /*
                         * 如果搜索长度为1的非数字的字符串，格式化为通配符搜索，暂时这样， 以后有时间改成multifield搜索 ，就不需要通配符了
                         */
                        if (formatValue.length() == 1 && !Pattern.matches("[0-9]", formatValue)) {
                            formatValue = "*" + formatValue + "*";
                        }
                    }
                    terms.add(formatValue);
                }
                String qryValue = StringUtils.join(terms, " ");
                // 在搜索精确匹配时按精确走
                if (mySearchOption.getSearchType() == SearchOption.SearchType.term) {
                    queryBuilder = QueryBuilders.termsQuery(field, terms).boost(mySearchOption.getBoost());
                } else if (mySearchOption.getSearchType() == SearchOption.SearchType.querystring) {
                    QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders
                            .queryStringQuery(mySearchOption.getTermOperator() == SearchOption.TermOperator.AND
                                    ? "\\\"" + qryValue + "\\\""
                                    : qryValue)
                            .defaultOperator(mySearchOption.getTermOperator() == SearchOption.TermOperator.AND
                                    ? QueryStringQueryBuilder.DEFAULT_OPERATOR.AND
                                    : QueryStringQueryBuilder.DEFAULT_OPERATOR.OR)
                            .minimumShouldMatch(mySearchOption.getQueryStringPrecision());
                    queryBuilder = queryStringQueryBuilder.field(field).boost(mySearchOption.getBoost());
                } else if (mySearchOption.getSearchType() == SearchOption.SearchType.match) {
                    queryBuilder = QueryBuilders.matchPhraseQuery(field, qryValue).slop(MATCH_PHRASE_SLOP);

                }
            }
            return queryBuilder;
        } catch (Exception e) {
            throw new SearchRuntimeException("ES create builder error", e);
        }
    }

    private RangeQueryBuilder createRangeQueryBuilder(String field, List<Object> valuesSet) {
        // 这里需要判断下范围，是单值也可以
        if (null == valuesSet || valuesSet.isEmpty()) {
            return null;
        }
        Object[] values = valuesSet.toArray(new Object[valuesSet.size()]);

        boolean timeType = false;
        if (SearchOption.isDate(values[0])) {
            timeType = true;
        }
        String begin = "";
        String end = "";
        if (timeType) {
            /*
             * 如果时间类型的区间搜索出现问题，有可能是数据类型导致的：
             * （1）在监控页面（elasticsearch-head）中进行range搜索，看看什么结果，如果也搜索不出来，则：
             * （2）请确定mapping中是date类型，格式化格式是yyyy-MM-dd HH:mm:ss （3）请确定索引里的值是类似2012-01-01
             * 00:00:00的格式 （4）如果是从数据库导出的数据，请确定数据库字段是char或者varchar类型，而不是date类型（此类型可能会有问题）
             */
            begin = SearchOption.formatDate(this.dtFmt, values[0]);
            if (values.length > 1)
                end = SearchOption.formatDate(this.dtFmt, values[1]);
        } else {
            begin = null == values[0] ? null : values[0].toString();
            if (values.length > 1)
                end = null == values[1] ? null : values[1].toString();
            if (null != begin) {
                begin = begin.toString().trim().replaceAll("\\*", "");// 格式化搜索数据
                begin = begin.replaceAll("\\^", "");// 格式化搜索数据
                begin = QueryParser.escape(begin);
                begin = begin.replaceAll("\\\\-", "-");
            }
            if (null != end) {
                end = end.trim().replaceAll("\\*", "");// 格式化搜索数据
                end = end.replaceAll("\\^", "");// 格式化搜索数据
                end = QueryParser.escape(end);
                end = end.replaceAll("\\\\-", "-");
            }
        }
        if (StringUtil.isBlank(end))
            return QueryBuilders.rangeQuery(field).from(begin);
        else
            return QueryBuilders.rangeQuery(field).from(begin).to(end);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private <T> T fromJson(String source, Class clazz, TypeGetter typeGetter) {
        T t = null;
        if (null != clazz && !clazz.isAssignableFrom(String.class)) {
            try {
                t = (T) gson.fromJson(source, clazz);
            } catch (Exception e) {
                t = (T) simpleGson.fromJson(source, clazz);
            }
        } else if (null != typeGetter) {
            try {
                t = gson.fromJson(source, typeGetter.getType());
            } catch (Exception e) {
                t = simpleGson.fromJson(source, typeGetter.getType());
            }
        } else {
            t = (T) source;
        }
        return t;

    }

    public <T> List<T> getSearchResult(RestHighLevelClient client, Scroll scroll, SearchResponse response,
            Class<T> clazz, @SuppressWarnings("rawtypes") TypeGetter typeGetter, int from, int offset) {
        try {
            List<T> results = new ArrayList<>();
            SearchHits hits = response.getHits();
            if (hits.getTotalHits().value == 0) {
                return results;
            }

            int start = -1;
            while (true) {
                for (SearchHit hit : response.getHits().getHits()) {
                    start++;
                    // from， offset处
                    // Handle the hit...
                    if (start >= from && start < (from + offset)) {
                        String source = hit.getSourceAsString();
                        results.add(fromJson(source, clazz, typeGetter));
                    } else if (start >= (from + offset)) {
                        // 退到外层
                        break;
                    }
                }
                if (start >= (from + offset)) {
                    // 退出while
                    break;
                }
                SearchScrollRequest scrollRequest = new SearchScrollRequest(response.getScrollId());
                scrollRequest.scroll(scroll);
                response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                // Break condition: No hits are returned
                if (response.getHits().getHits().length == 0) {
                    break;
                }
            }
            return results;
        } catch (Exception e) {
            throw new SearchRuntimeException("ES search error", e);
        }
    }

    public <T extends GeoLocation> List<T> getGeoResult(RestHighLevelClient client, Scroll scroll,
            SearchResponse response, Class<T> clazz, @SuppressWarnings("rawtypes") TypeGetter typeGetter, int from,
            int offset) {
        try {
            List<T> results = new ArrayList<>();
            SearchHits hits = response.getHits();
            if (hits.getTotalHits().value == 0) {
                return results;
            }

            int start = -1;
            while (true) {
                for (SearchHit hit : response.getHits().getHits()) {
                    start++;
                    // from， offset处
                    // Handle the hit...
                    if (start >= from && start < (from + offset)) {
                        String source = hit.getSourceAsString();
                        T t = fromJson(source, clazz, typeGetter);
                        // 加上地理的距离
                        t.setDistance((double) hit.getSortValues()[0]);
                        results.add(t);
                    } else if (start >= (from + offset)) {
                        // 退到外层
                        break;
                    }
                }
                if (start >= (from + offset)) {
                    // 退出while
                    break;
                }
                SearchScrollRequest scrollRequest = new SearchScrollRequest(response.getScrollId());
                scrollRequest.scroll(scroll);
                response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                // Break condition: No hits are returned
                if (response.getHits().getHits().length == 0) {
                    break;
                }
            }
            return results;
        } catch (Exception e) {
            throw new SearchRuntimeException("ES search error", e);
        }
    }

    public String getSearchResult(SearchResponse response) {
        try {
            SearchHits hits = response.getHits();
            if (hits.getTotalHits().value == 0) {
                return null;
            }
            String source = null;
            List<String> results = new ArrayList<>();
            for (SearchHit searchHit : hits.getHits()) {
                source = searchHit.getSourceAsString();
                results.add(source);
            }
            return gson.toJson(results);
        } catch (Exception e) {
            throw new SearchRuntimeException("ES search error", e);
        }
    }

    public SearchSourceBuilder createHighlight(SearchSourceBuilder searchSourceBuilder,
            List<SearchCriteria> searchCriterias, String highlightCSS) {
        for (SearchCriteria searchCriteria : searchCriterias) {
            String field = getLowerFormatField(searchCriteria.getField());
            SearchOption searchOption = searchCriteria.getOption();
            if (searchOption.isHighlight()) {
                /*
                 * http://www.elasticsearch.org/guide/reference/api/search/ highlighting.html
                 * 
                 * fragment_size设置成1000，默认值会造成返回的数据被截断
                 */
                HighlightBuilder highlightBuilder = new HighlightBuilder().field(field, 1000)
                        .preTags("<" + highlightCSS.split(",")[0] + ">")
                        .postTags("</" + highlightCSS.split(",")[1] + ">");
                searchSourceBuilder.highlighter(highlightBuilder);

            }
        }

        return searchSourceBuilder;
    }

    public TermsAggregationBuilder addSubAggs(TermsAggregationBuilder termBuilder, List<AggField> subFields) {
        if (null == subFields || subFields.isEmpty())
            return termBuilder;
        for (AggField field : subFields) {
            termBuilder
                    .subAggregation(
                            AggregationBuilders.terms(field.getField() + "_aggs").field(field.getField() + ".raw"))
                    .size(100);
            termBuilder = addSubAggs(termBuilder, field.getSubAggs());
        }
        return termBuilder;
    }

    public List<AggResult> getAgg(SearchResponse searchResponse, List<AggField> fields) {
        List<AggResult> aggResults = new ArrayList<>();
        for (AggField field : fields) {
            Terms sortAggregate = searchResponse.getAggregations().get(field.getField() + "_aggs");
            if (null != sortAggregate && null != sortAggregate.getBuckets()) {
                for (Terms.Bucket entry : sortAggregate.getBuckets()) {
                    // 新建一个对象
                    AggResult aggResult = new AggResult(entry.getKeyAsString(), entry.getDocCount(), field.getField());
                    // 嵌套循环
                    List<AggResult> temp = getSubAgg(entry, field.getSubAggs());
                    aggResult.setSubResult(temp);
                    // 加到list
                    aggResults.add(aggResult);
                }
            }
        }
        return aggResults;
    }

    private List<AggResult> getSubAgg(Terms.Bucket entry, List<AggField> fields) {
        List<AggResult> aggResults = new ArrayList<>();
        if (null == entry || null == fields || fields.isEmpty())
            return aggResults;

        for (AggField field : fields) {
            Terms subAgg = entry.getAggregations().get(field.getField() + "_aggs");
            if (null != subAgg) {
                aggResults = new ArrayList<>();
                for (Terms.Bucket subEntry : subAgg.getBuckets()) {
                    AggResult aggResult = new AggResult(subEntry.getKeyAsString(), entry.getDocCount(),
                            field.getField());
                    // 嵌套循环
                    List<AggResult> temp = getSubAgg(entry, field.getSubAggs());
                    if (null != temp)
                        aggResult.setSubResult(temp);
                    // 加到list
                    aggResults.add(aggResult);
                }
            }
        }
        return aggResults;
    }

}
