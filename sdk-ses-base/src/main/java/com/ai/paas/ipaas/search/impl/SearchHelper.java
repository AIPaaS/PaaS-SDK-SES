package com.ai.paas.ipaas.search.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;

import com.ai.paas.ipaas.search.SearchRuntimeException;
import com.ai.paas.ipaas.search.vo.AggField;
import com.ai.paas.ipaas.search.vo.AggResult;
import com.ai.paas.ipaas.search.vo.SearchCriteria;
import com.ai.paas.ipaas.search.vo.SearchOption;
import com.ai.paas.ipaas.util.StringUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class SearchHelper {
	private SearchHelper() {

	}

	private static Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss").create();

	public static String getId(String json, String id) {
		JsonObject obj = gson.fromJson(json, JsonObject.class);
		if (null != obj.get(id))
			return obj.get(id).getAsString();
		else
			return null;
	}

	public static String getId(XContentBuilder builder, String id) {
		String json;
		try {
			json = builder.string();
			return getId(json, id);
		} catch (IOException e) {
			throw new SearchRuntimeException(builder.toString(), e);
		}
	}

	public static boolean hasId(XContentBuilder builder, String id) {
		String json;
		try {
			json = builder.string();
			return hasId(json, id);
		} catch (IOException e) {
			throw new SearchRuntimeException(builder.toString(), e);
		}
	}

	public static boolean hasId(String json, String id) {

		JsonObject obj = gson.fromJson(json, JsonObject.class);
		if (null != obj.get(id))
			return true;
		return false;
	}

	public static QueryBuilder createStringSQLBuilder(String query) {
		try {
			if (StringUtil.isBlank(query)) {
				return null;
			}
			QueryStringQueryBuilder qsqb = QueryBuilders.queryStringQuery(query);
			return qsqb;
		} catch (Exception e) {
			throw new SearchRuntimeException("ES create builder error", e);

		}
	}

	/**
	 * 搜索条件，支持嵌套逻辑
	 * 
	 * @param searchCriterias
	 * @return 索引对象
	 */
	public static QueryBuilder createQueryBuilder(List<SearchCriteria> searchCriterias) {
		try {
			if (searchCriterias == null || searchCriterias.size() == 0) {
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

				String field = getLowerFormatField(searchCriteria.getField());
				if (!StringUtil.isBlank(field)) {
					SearchOption searchOption = searchCriteria.getOption();
					QueryBuilder queryBuilder = createSingleFieldQueryBuilder(field, searchCriteria.getFieldValue(),
							searchOption);
					if (queryBuilder != null) {
						searchCriteria.getOption().getSearchLogic().convertQueryBuilder(rootQueryBuilder, queryBuilder);
					}
				}

			}
			return rootQueryBuilder;
		} catch (Exception e) {
			throw new SearchRuntimeException("ES create builder error", e);

		}
	}

	private static String getLowerFormatField(String field) {
		if (StringUtil.isBlank(field))
			return null;
		return field;
	}

	/*
	 * 创建过滤条件
	 */
	public static QueryBuilder createSingleFieldQueryBuilder(String field, List<Object> values,
			SearchOption mySearchOption) {
		try {
			if (mySearchOption.getSearchType() == SearchOption.SearchType.range) {
				/* 区间搜索 */
				return createRangeQueryBuilder(field, values);
			}
			QueryBuilder queryBuilder = null;
			if (values != null) {
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
					formatValue = StringEscapeUtils.escapeJson(formatValue);
					if (mySearchOption.getSearchType() == SearchOption.SearchType.querystring) {
						if (formatValue.length() == 1) {
							/*
							 * 如果搜索长度为1的非数字的字符串，格式化为通配符搜索，暂时这样，
							 * 以后有时间改成multifield搜索 ，就不需要通配符了
							 */
							if (!Pattern.matches("[0-9]", formatValue)) {
								formatValue = "*" + formatValue + "*";
							}
						}
					}
					terms.add(formatValue);
				}
				if (mySearchOption.getSearchType() == SearchOption.SearchType.term) {
					queryBuilder = QueryBuilders.termsQuery(field, terms).boost(mySearchOption.getBoost());
				} else if (mySearchOption.getSearchType() == SearchOption.SearchType.querystring) {
					QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders
							.queryStringQuery(StringUtils.join(terms, " "))
							.defaultOperator(mySearchOption.getTermOperator() == SearchOption.TermOperator.AND
									? QueryStringQueryBuilder.Operator.AND : QueryStringQueryBuilder.Operator.OR)
							.minimumShouldMatch(mySearchOption.getQueryStringPrecision());
					queryBuilder = queryStringQueryBuilder.field(field).boost(mySearchOption.getBoost());
				} else if (mySearchOption.getSearchType() == SearchOption.SearchType.match) {
					queryBuilder = QueryBuilders.matchQuery(field, StringUtils.join(terms, " "))
							.operator(mySearchOption.getTermOperator() == SearchOption.TermOperator.AND
									? MatchQueryBuilder.Operator.AND : MatchQueryBuilder.Operator.OR)
							.minimumShouldMatch("90%").type(MatchQueryBuilder.Type.PHRASE_PREFIX);

				}
			}
			return queryBuilder;
		} catch (Exception e) {
			throw new SearchRuntimeException("ES create builder error", e);
		}
	}

	private static RangeQueryBuilder createRangeQueryBuilder(String field, List<Object> valuesSet) {
		// 这里需要判断下范围，是单值也可以
		if (null == valuesSet || valuesSet.size() == 0) {
			return null;
		}
		Object[] values = (Object[]) valuesSet.toArray(new Object[valuesSet.size()]);

		boolean timeType = false;
		if (SearchOption.isDate(values[0])) {
			timeType = true;
		}
		String begin = "", end = "";
		if (timeType) {
			/*
			 * 如果时间类型的区间搜索出现问题，有可能是数据类型导致的：
			 * （1）在监控页面（elasticsearch-head）中进行range搜索，看看什么结果，如果也搜索不出来，则：
			 * （2）请确定mapping中是date类型，格式化格式是yyyy-MM-dd HH:mm:ss
			 * （3）请确定索引里的值是类似2012-01-01 00:00:00的格式
			 * （4）如果是从数据库导出的数据，请确定数据库字段是char或者varchar类型，而不是date类型（此类型可能会有问题）
			 */
			begin = SearchOption.formatDate(values[0]);
			end = SearchOption.formatDate(values[1]);
		} else {
			begin = null == values[0] ? null : values[0].toString();
			end = null == values[1] ? null : values[1].toString();
			if (null != begin) {
				begin = begin.toString().trim().replaceAll("\\*", "");// 格式化搜索数据
				begin = begin.replaceAll("\\^", "");// 格式化搜索数据
				begin = StringEscapeUtils.escapeJson(begin);
			}
			if (null != end) {
				end = end.toString().trim().replaceAll("\\*", "");// 格式化搜索数据
				end = end.replaceAll("\\^", "");// 格式化搜索数据
				end = StringEscapeUtils.escapeJson(end);
			}
		}
		return QueryBuilders.rangeQuery(field).from(begin).to(end);
	}

	@SuppressWarnings("unchecked")
	public static <T> List<T> getSearchResult(TransportClient client, SearchResponse response, Class<T> clazz, int from,
			int offset) {
		try {
			List<T> results = new ArrayList<>();
			SearchHits hits = response.getHits();
			if (hits.getTotalHits() == 0) {
				return results;
			}
			int start = -1;
			while (true) {
				for (SearchHit hit : response.getHits().getHits()) {
					start++;
					// from， offset处
					// Handle the hit...
					if (start >= from && start < (from + offset)) {
						// 找到位置，开始存储，到偏移就结束
						String source = hit.getSourceAsString();
						if (null != clazz && !clazz.getName().equals(String.class.getName()))
							results.add(gson.fromJson(source, clazz));
						else
							results.add((T) source);
					} else if (start >= (from + offset)) {
						// 退到外层
						break;
					}
				}
				if (start >= (from + offset)) {
					// 退出while
					break;
				}
				response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute()
						.actionGet();
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

	public static String getSearchResult(SearchResponse response) {
		try {
			SearchHits hits = response.getHits();
			if (hits.getTotalHits() == 0) {
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

	public static SearchRequestBuilder createHighlight(SearchRequestBuilder searchRequestBuilder,
			List<SearchCriteria> searchCriterias, String highlightCSS) {
		for (SearchCriteria searchCriteria : searchCriterias) {
			String field = getLowerFormatField(searchCriteria.getField());
			SearchOption searchOption = searchCriteria.getOption();
			if (searchOption.isHighlight()) {
				/*
				 * http://www.elasticsearch.org/guide/reference/api/search/
				 * highlighting.html
				 * 
				 * fragment_size设置成1000，默认值会造成返回的数据被截断
				 */
				searchRequestBuilder = searchRequestBuilder.addHighlightedField(field, 1000)
						.setHighlighterPreTags("<" + highlightCSS.split(",")[0] + ">")
						.setHighlighterPostTags("</" + highlightCSS.split(",")[1] + ">");

			}
		}

		return searchRequestBuilder;
	}

	public static List<AggResult> getAgg(SearchResponse searchResponse, List<AggField> fields) {
		List<AggResult> aggResults = new ArrayList<>();
		for (AggField field : fields) {
			Terms sortAggregate = searchResponse.getAggregations().get(field.getField() + "_aggs");
			if (null != sortAggregate && null != sortAggregate.getBuckets()) {
				for (Terms.Bucket entry : sortAggregate.getBuckets()) {
					// 新建一个对象
					AggResult aggResult = new AggResult(entry.getKeyAsString(), entry.getDocCount(), field.getField());
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

	private static List<AggResult> getSubAgg(Terms.Bucket entry, List<AggField> fields) {
		if (null == entry || null == fields || fields.size() <= 0)
			return null;
		List<AggResult> aggResults = null;
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

	public static TermsBuilder addSubAggs(TermsBuilder termBuilder, List<AggField> subFields) {
		if (null == subFields || subFields.size() <= 0)
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

	public static void main(String[] args) {

	}

}
