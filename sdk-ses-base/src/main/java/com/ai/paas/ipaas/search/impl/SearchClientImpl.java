package com.ai.paas.ipaas.search.impl;

//搜索实现定义

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortParseElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.PaaSConstant;
import com.ai.paas.ipaas.search.ISearchClient;
import com.ai.paas.ipaas.search.SearchRuntimeException;
import com.ai.paas.ipaas.search.common.JsonBuilder;
import com.ai.paas.ipaas.search.vo.AggField;
import com.ai.paas.ipaas.search.vo.AggResult;
import com.ai.paas.ipaas.search.vo.Result;
import com.ai.paas.ipaas.search.vo.SearchCriteria;
import com.ai.paas.ipaas.search.vo.Sort;
import com.ai.paas.ipaas.util.Assert;
import com.ai.paas.ipaas.util.StringUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;

public class SearchClientImpl implements ISearchClient {
	private Logger logger = LoggerFactory.getLogger(SearchClientImpl.class);
	private String highlightCSS = "span,span";
	private String indexName;
	private String _id = null;
	private static Settings settings = Settings.settingsBuilder().put("client.transport.ping_timeout", "60s")
			.put("client.transport.sniff", "true").put("client.transport.ignore_cluster_name", "true").build();
	private String hosts = null;
	// 创建私有对象
	private TransportClient client;

	public SearchClientImpl(String hosts, String indexName, String id) {
		this.indexName = indexName;
		_id = id;
		this.hosts = hosts;
		initClient();
	}

	public void initClient() {
		List<String> clusterList = new ArrayList<String>();
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
			logger.error(e.getMessage(), e);
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

	private SearchResponse count(String indexName, Object queryBuilder) {
		try {
			SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName).setSize(0);
			if (queryBuilder instanceof QueryBuilder) {
				searchRequestBuilder = searchRequestBuilder.setQuery((QueryBuilder) queryBuilder);
			}
			if (queryBuilder instanceof byte[]) {
				String query = new String((byte[]) queryBuilder);
				searchRequestBuilder = searchRequestBuilder.setQuery(QueryBuilders.wrapperQuery(query));
			}
			return searchRequestBuilder.get();
		} catch (Exception e) {
			logger.error("search count error", e);
			throw new SearchRuntimeException("ES search count error", e);
		}
	}

	public List<String> getSuggest(String value, int count) {
		return getSuggest("_all", value, count);
	}

	public List<String> getSuggest(String field, String value, int count) {
		if (StringUtil.isBlank(field) || StringUtil.isBlank(value) || count <= 0)
			return null;
		SearchResponse response = client.prepareSearch(indexName)
				.setQuery(QueryBuilders.matchQuery(field, value).operator(Operator.AND)).setFrom(0).setSize(count)
				.get();
		if (null == response)
			return null;
		SearchHits hits = response.getHits();
		if (hits.getTotalHits() == 0) {
			return null;
		}
		List<String> suggests = new ArrayList<>();
		for (SearchHit searchHit : hits.getHits()) {
			suggests.add(searchHit.getSourceAsString());
		}
		return suggests;
	}

	@Override
	public boolean insert(Map<String, Object> data) {
		if (null == data || data.size() <= 0)
			return false;
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ").create();
		return insert(gson.toJson(data));
	}

	@Override
	public boolean insert(String json) {
		if (StringUtil.isBlank(json))
			return false;
		IndexResponse response = null;
		// 判断一下是否有id字段
		String id = SearchHelper.getId(json, _id);
		if (StringUtil.isBlank(id)) {
			response = client.prepareIndex(indexName, indexName).setOpType(IndexRequest.OpType.CREATE).setSource(json)
					.get();
		} else {
			response = client.prepareIndex(indexName, indexName, id).setOpType(IndexRequest.OpType.CREATE)
					.setSource(json).setRefresh(true).get();
		}
		if (null != response && response.isCreated()) {
			return true;
		} else {
			throw new SearchRuntimeException("index error!" + json, response.toString());
		}
	}

	@Override
	public <T> boolean insert(T data) {
		if (null == data)
			return false;
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ").create();
		return insert(gson.toJson(data));
	}

	@Override
	public boolean insert(JsonBuilder jsonBuilder) {
		if (null == jsonBuilder)
			return false;
		// 判断是否有id
		XContentBuilder builder = null;
		try {
			builder = jsonBuilder.getBuilder();
			SearchHelper.hasId(builder, _id);
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
		if (null != response && response.isFound())
			return true;
		return false;
	}

	@Override
	public boolean bulkDelete(List<String> ids) {
		if (null == ids || ids.size() <= 0)
			return false;
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		for (String id : ids) {
			bulkRequest.add(client.prepareDelete(indexName, indexName, id));
		}
		BulkResponse bulkResponse = bulkRequest.setRefresh(true).get();
		if (!bulkResponse.hasFailures()) {
			return true;
		} else {
			// 这里要做个日志，哪些成功了
			for (BulkItemResponse response : bulkResponse.getItems()) {
				logger.error("Doc id:" + response.getId() + " is falided:" + response.isFailed());
			}
			return false;
		}
	}

	@Override
	public boolean delete(List<SearchCriteria> searchCriteria) {
		if (null == searchCriteria || searchCriteria.size() <= 0)
			return false;
		// 此处要先scan出来，然后再批量删除
		List<String> ids = new ArrayList<>();
		QueryBuilder queryBuilder = null;
		queryBuilder = SearchHelper.createQueryBuilder(searchCriteria);
		SearchResponse scrollResp = client.prepareSearch(indexName)
				.addSort(SortParseElement.DOC_FIELD_NAME, SortOrder.ASC).setScroll(new TimeValue(60000))
				.setQuery(queryBuilder).setSize(100).execute().actionGet();
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
		return bulkDelete(ids);
	}

	@Override
	public boolean clean() {
		// 全部删除，只能清除index，然后创建？
		// 先取出type定义
		GetMappingsResponse mappings;
		GetSettingsResponse settings;
		try {
			settings = client.admin().indices().getSettings(new GetSettingsRequest().indices(indexName)).get();
			mappings = client.admin().indices().getMappings(new GetMappingsRequest().indices(indexName)).get();
			DeleteIndexResponse delete = client.admin().indices().delete(new DeleteIndexRequest(indexName)).get();
			if (!delete.isAcknowledged()) {
				logger.error("Index wasn't deleted");
				return false;
			} else {
				// 看看是否包含这个type
				if (mappings.getMappings().containsKey(indexName)) {

					CreateIndexResponse indexResponse = client.admin().indices().prepareCreate(indexName)
							.setSettings(settings.getIndexToSettings().get(indexName)).get();

					if (indexResponse.isAcknowledged()) {
						PutMappingResponse putMappingResponse = client.admin().indices().preparePutMapping()
								.setIndices(indexName).setType(indexName)
								.setSource(mappings.getMappings().get(indexName).get(indexName).source().string())
								.get();
						if (putMappingResponse.isAcknowledged())
							return true;
						else
							return false;
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
		if (!StringUtil.isBlank(response.getId()))
			return true;
		else
			return false;
	}

	@Override
	public boolean update(String id, String json) {
		if (StringUtil.isBlank(id) || StringUtil.isBlank(json))
			return false;
		UpdateResponse response = client.prepareUpdate(indexName, indexName, id).setRefresh(true)
				.setConsistencyLevel(WriteConsistencyLevel.DEFAULT).setDoc(json).setRefresh(true).get();
		if (!StringUtil.isBlank(response.getId()))
			return true;
		else
			return false;
	}

	@Override
	public <T> boolean update(String id, T data) {
		if (StringUtil.isBlank(id) || null == data)
			return false;
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ").create();
		return update(id, gson.toJson(data));
	}

	@Override
	public boolean update(String id, JsonBuilder jsonBuilder) {
		if (StringUtil.isBlank(id) || null == jsonBuilder)
			return false;
		UpdateResponse response = client.prepareUpdate(indexName, indexName, id).setRefresh(true)
				.setConsistencyLevel(WriteConsistencyLevel.DEFAULT).setDoc(jsonBuilder.getBuilder()).setRefresh(true)
				.get();
		if (!StringUtil.isBlank(response.getId()))
			return true;
		else
			return false;
	}

	@Override
	public boolean upsert(String id, Map<String, Object> data) {
		if (StringUtil.isBlank(id) || null == data || data.size() <= 0)
			return false;
		UpdateResponse response = client.prepareUpdate(indexName, indexName, id).setRefresh(true)
				.setConsistencyLevel(WriteConsistencyLevel.DEFAULT).setUpsert(data).setDoc(data).setRefresh(true).get();
		if (!StringUtil.isBlank(response.getId()))
			return true;
		else
			return false;
	}

	@Override
	public boolean upsert(String id, String json) {
		if (StringUtil.isBlank(id) || StringUtil.isBlank(json))
			return false;
		UpdateResponse response = client.prepareUpdate(indexName, indexName, id).setRefresh(true)
				.setConsistencyLevel(WriteConsistencyLevel.DEFAULT).setUpsert(json).setDoc(json).setRefresh(true).get();
		if (!StringUtil.isBlank(response.getId()))
			return true;
		else
			return false;
	}

	@Override
	public <T> boolean upsert(String id, T data) {
		if (StringUtil.isBlank(id) || null == data)
			return false;
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ").create();
		return upsert(id, gson.toJson(data));
	}

	@Override
	public boolean upsert(String id, JsonBuilder jsonBuilder) {
		if (StringUtil.isBlank(id) || null == jsonBuilder)
			return false;
		UpdateResponse response = client.prepareUpdate(indexName, indexName, id).setRefresh(true)
				.setConsistencyLevel(WriteConsistencyLevel.DEFAULT).setUpsert(jsonBuilder.getBuilder())
				.setDoc(jsonBuilder.getBuilder()).setRefresh(true).get();
		if (!StringUtil.isBlank(response.getId()))
			return true;
		else
			return false;
	}

	private void logBulkRespone(BulkResponse bulkResponse) {
		for (BulkItemResponse response : bulkResponse.getItems()) {
			logger.error("insert error," + response.getId() + "," + response.getFailure());
		}
	}

	@Override
	public boolean bulkMapInsert(List<Map<String, Object>> datas) {
		if (null == datas || datas.size() <= 0)
			return false;
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		BulkResponse bulkResponse = null;
		for (Map<String, Object> data : datas) {
			if (null != data.get(_id)) {
				bulkRequest.add(client.prepareIndex(indexName, indexName, data.get(_id).toString()).setSource(data));
			} else {
				bulkRequest.add(client.prepareIndex(indexName, indexName).setSource(data));
			}
			if (bulkRequest.numberOfActions() > 100) {
				bulkResponse = bulkRequest.get();
				logger.info("rebuildIndex(): indexed {}, hasFailures: {}", bulkRequest.numberOfActions(),
						bulkResponse.hasFailures());
			}
		}
		if (bulkRequest.numberOfActions() > 0) {
			bulkResponse = bulkRequest.setRefresh(true).get();
		}
		if (!bulkResponse.hasFailures()) {
			return true;
		} else {
			logger.error("insert error", bulkResponse.buildFailureMessage());
			logBulkRespone(bulkResponse);
			throw new SearchRuntimeException("insert error", "insert error" + bulkResponse.buildFailureMessage());
		}
	}

	@Override
	public boolean bulkJsonInsert(List<String> jsons) {
		if (null == jsons || jsons.size() <= 0)
			return false;
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		BulkResponse bulkResponse = null;
		for (String json : jsons) {
			if (SearchHelper.hasId(json, _id)) {
				bulkRequest.add(client.prepareIndex(indexName, indexName, SearchHelper.getId(json, _id).toString())
						.setSource(json));
			} else {
				bulkRequest.add(client.prepareIndex(indexName, indexName).setSource(json));
			}
			if (bulkRequest.numberOfActions() > 100) {
				bulkResponse = bulkRequest.setRefresh(true).get();
				logger.info("rebuildIndex(): indexed {}, hasFailures: {}", bulkRequest.numberOfActions(),
						bulkResponse.hasFailures());
			}
		}
		if (bulkRequest.numberOfActions() > 0) {
			bulkResponse = bulkRequest.setRefresh(true).get();
		}
		if (!bulkResponse.hasFailures()) {
			return true;
		} else {
			this.logger.error("insert error", bulkResponse.buildFailureMessage());
			logBulkRespone(bulkResponse);
			throw new SearchRuntimeException("insert error", "insert error" + bulkResponse.buildFailureMessage());
		}
	}

	@Override
	public <T> boolean bulkInsert(List<T> datas) {
		if (null == datas || datas.size() <= 0)
			return false;
		List<String> jsons = new ArrayList<>();
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ").create();
		for (T t : datas) {
			jsons.add(gson.toJson(t));
		}
		return bulkJsonInsert(jsons);
	}

	@Override
	public boolean bulkInsert(Set<JsonBuilder> jsonBuilders) {
		if (null == jsonBuilders || jsonBuilders.size() <= 0)
			return false;
		BulkRequestBuilder bulkRequest = client.prepareBulk();
		BulkResponse bulkResponse = null;
		for (JsonBuilder jsonBuilder : jsonBuilders) {
			if (SearchHelper.hasId(jsonBuilder.getBuilder(), _id)) {
				bulkRequest.add(client
						.prepareIndex(indexName, indexName,
								SearchHelper.getId(jsonBuilder.getBuilder(), _id).toString())
						.setSource(jsonBuilder.getBuilder()));
			} else {
				bulkRequest.add(client.prepareIndex(indexName, indexName).setSource(jsonBuilder.getBuilder()));
			}
			if (bulkRequest.numberOfActions() > 100) {
				bulkResponse = bulkRequest.setRefresh(true).get();
				logger.info("rebuildIndex(): indexed {}, hasFailures: {}", bulkRequest.numberOfActions(),
						bulkResponse.hasFailures());
			}
		}
		if (bulkRequest.numberOfActions() > 0) {
			bulkResponse = bulkRequest.setRefresh(true).get();
		}
		if (!bulkResponse.hasFailures()) {
			return true;
		} else {
			this.logger.error("insert error", bulkResponse.buildFailureMessage());
			logBulkRespone(bulkResponse);
			throw new SearchRuntimeException("insert error", "insert error" + bulkResponse.buildFailureMessage());
		}
	}

	@Override
	public boolean bulkMapUpdate(List<String> ids, List<Map<String, Object>> datas) {
		if (null == ids || null == datas || ids.size() != datas.size())
			throw new SearchRuntimeException("Null parameters or size not equal!");
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		int i = 0;
		for (String documentId : ids) {
			bulkRequestBuilder.add(client.prepareUpdate(indexName, indexName, documentId).setUpsert(datas.get(i))
					.setDoc(datas.get(i++)));
		}

		BulkResponse bulkResponse = bulkRequestBuilder.setRefresh(true).get();
		if (!bulkResponse.hasFailures())
			return true;
		else {
			this.logger.error("update error", bulkResponse.buildFailureMessage());
			logBulkRespone(bulkResponse);
			throw new SearchRuntimeException("update error", "update error" + bulkResponse.buildFailureMessage());
		}
	}

	@Override
	public boolean bulkJsonUpdate(List<String> ids, List<String> jsons) {
		if (null == ids || null == jsons || ids.size() != jsons.size())
			throw new SearchRuntimeException("Null parameters or size not equal!");
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		int i = 0;
		for (String documentId : ids) {
			bulkRequestBuilder.add(client.prepareUpdate(indexName, indexName, documentId).setUpsert(jsons.get(i))
					.setDoc(jsons.get(i++)));
		}

		BulkResponse bulkResponse = bulkRequestBuilder.setRefresh(true).get();
		if (!bulkResponse.hasFailures())
			return true;
		else {
			this.logger.error("update error", bulkResponse.buildFailureMessage());
			logBulkRespone(bulkResponse);
			throw new SearchRuntimeException("update error", "update error" + bulkResponse.buildFailureMessage());
		}
	}

	@Override
	public <T> boolean bulkUpdate(List<String> ids, List<T> datas) {
		if (null == ids || null == datas || ids.size() != datas.size())
			throw new SearchRuntimeException("Null parameters or size not equal!");
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		int i = 0;
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ").create();
		for (String documentId : ids) {
			bulkRequestBuilder.add(client.prepareUpdate(indexName, indexName, documentId)
					.setUpsert(gson.toJson(datas.get(i))).setDoc(gson.toJson(datas.get(i++))));
		}

		BulkResponse bulkResponse = bulkRequestBuilder.setRefresh(true).get();
		if (!bulkResponse.hasFailures())
			return true;
		else {
			this.logger.error("update error", bulkResponse.buildFailureMessage());
			logBulkRespone(bulkResponse);
			throw new SearchRuntimeException("update error", "update error" + bulkResponse.buildFailureMessage());
		}
	}

	@Override
	public boolean bulkUpdate(List<String> ids, Set<JsonBuilder> jsonBuilders) {
		if (null == ids || null == jsonBuilders || ids.size() != jsonBuilders.size())
			throw new SearchRuntimeException("Null parameters or size not equal!");
		BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
		int i = 0;
		for (JsonBuilder jsonBuilder : jsonBuilders) {
			bulkRequestBuilder.add(client.prepareUpdate(indexName, indexName, ids.get(i++))
					.setUpsert(jsonBuilder.getBuilder()).setDoc(jsonBuilder.getBuilder()));
		}
		BulkResponse bulkResponse = bulkRequestBuilder.setRefresh(true).get();
		if (!bulkResponse.hasFailures())
			return true;
		else {
			this.logger.error("update error", bulkResponse.buildFailureMessage());
			logBulkRespone(bulkResponse);
			throw new SearchRuntimeException("update error", "update error" + bulkResponse.buildFailureMessage());
		}
	}

	@Override
	public boolean bulkMapUpsert(List<String> ids, List<Map<String, Object>> datas) {
		return bulkMapUpdate(ids, datas);
	}

	@Override
	public boolean bulkJsonUpsert(List<String> ids, List<String> jsons) {
		return bulkJsonUpdate(ids, jsons);
	}

	@Override
	public <T> boolean bulkUpsert(List<String> ids, List<T> datas) {
		return bulkUpdate(ids, datas);
	}

	@Override
	public boolean bulkUpsert(List<String> ids, Set<JsonBuilder> jsonBuilders) {
		return bulkUpsert(ids, jsonBuilders);
	}

	private <T> Result<T> search(QueryBuilder queryBuilder, int from, int offset, List<Sort> sorts, Class<T> clazz,
			List<SearchCriteria> searchCriterias) {
		Result<T> result = new Result<>();
		result.setResultCode(PaaSConstant.ExceptionCode.SYSTEM_ERROR);
		try {
			/* 查询搜索总数 */
			SearchResponse searchResponse = count(indexName, queryBuilder);
			long count = searchResponse.getHits().totalHits();

			SearchRequestBuilder searchRequestBuilder = null;
			searchRequestBuilder = client.prepareSearch(indexName).setSearchType(SearchType.DEFAULT)
					.setQuery(queryBuilder).setFrom(from).setSize(offset).setExplain(true);
			if (sorts == null || sorts.isEmpty()) {
				/* 如果不需要排序 */
			} else {
				/* 如果需要排序 */
				for (Sort sort : sorts) {
					SortOrder sortOrder = sort.getOrder().compareTo(Sort.SortOrder.DESC) == 0 ? SortOrder.DESC
							: SortOrder.ASC;

					searchRequestBuilder = searchRequestBuilder.addSort(sort.getSortBy().toLowerCase(), sortOrder);
				}
			}
			// 增加高亮
			if (null != searchCriterias) {
				searchRequestBuilder = SearchHelper.createHighlight(searchRequestBuilder, searchCriterias,
						highlightCSS);
			}
			searchResponse = searchRequestBuilder.get();
			List<T> list = SearchHelper.getSearchResult(searchResponse, clazz);

			result.setContents(list);
			result.setCounts(count);
			result.setResultCode(PaaSConstant.RPC_CALL_OK);
		} catch (Exception e) {
			this.logger.error(e.getMessage(), e);
			throw new SearchRuntimeException("ES searchIndex error", e);
		}
		return result;
	}

	@Override
	public <T> Result<T> searchBySQL(String query, int from, int offset, List<Sort> sorts, Class<T> clazz) {
		// 创建query
		QueryBuilder queryBuilder = SearchHelper.createStringSQLBuilder(query);
		return search(queryBuilder, from, offset, sorts, clazz, null);
	}

	@Override
	public <T> Result<T> search(List<SearchCriteria> searchCriterias, int from, int offset, List<Sort> sorts,
			Class<T> clazz) {
		// 创建query
		QueryBuilder queryBuilder = SearchHelper.createQueryBuilder(searchCriterias);
		return search(queryBuilder, from, offset, sorts, clazz, searchCriterias);
	}

	public <T> Result<T> searchByDSL(String dslJson, int from, int offset, @Nullable List<Sort> sorts, Class<T> clazz) {
		QueryBuilder queryBuilder = QueryBuilders.wrapperQuery(dslJson);
		return search(queryBuilder, from, offset, sorts, clazz, null);
	}

	@Override
	public Result<Map<String, Long>> aggregate(List<SearchCriteria> searchCriterias, String field) {
		Result<Map<String, Long>> result = new Result<Map<String, Long>>();
		result.setResultCode(PaaSConstant.ExceptionCode.SYSTEM_ERROR);
		try {
			QueryBuilder queryBuilder = SearchHelper.createQueryBuilder(searchCriterias);
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
			logger.error(e.getMessage(), e);
			throw new SearchRuntimeException("ES simpleAggregation error", e);
		}
		return result;
	}

	@Override
	public Result<List<AggResult>> aggregate(List<SearchCriteria> searchCriterias, List<AggField> fields) {
		if (null == searchCriterias || null == fields || searchCriterias.size() <= 0 || fields.size() <= 0)
			throw new SearchRuntimeException("IllegelArguments! null");
		Result<List<AggResult>> result = new Result<List<AggResult>>();
		result.setResultCode(PaaSConstant.ExceptionCode.SYSTEM_ERROR);
		try {
			QueryBuilder queryBuilder = SearchHelper.createQueryBuilder(searchCriterias);
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
				termBuilder = SearchHelper.addSubAggs(termBuilder, aggField.getSubAggs());
				searchRequestBuilder.addAggregation(termBuilder);
			}
			SearchResponse searchResponse = searchRequestBuilder.setSize(0).get();

			result.setCounts(searchResponse.getHits().getTotalHits());
			result.setAggs(SearchHelper.getAgg(searchResponse, fields));
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new SearchRuntimeException("ES simpleAggregation error", e);
		}
		return result;
	}

	@Override
	public <T> Result<T> fullTextSearch(String text, int from, int offset, List<Sort> sorts, Class<T> clazz) {
		Result<T> result = new Result<>();
		result.setResultCode(PaaSConstant.ExceptionCode.SYSTEM_ERROR);
		try {
			SearchRequestBuilder searchRequestBuilder = client.prepareSearch(indexName)
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
					.setQuery(QueryBuilders.matchQuery("_all", text).operator(Operator.AND).minimumShouldMatch("75%"))
					.setFrom(from).setSize(offset).setExplain(true).setHighlighterRequireFieldMatch(true);
			SearchResponse response = searchRequestBuilder.get();

			List<T> list = SearchHelper.getSearchResult(response, clazz);

			result.setContents(list);
			result.setCounts(response.getHits().totalHits());

			result.setResultCode(PaaSConstant.RPC_CALL_OK);
		} catch (Exception e) {
			this.logger.error(e.getMessage(), e);
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
					.setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(queryBuilder).setFrom(from)
					.setSize(offset);
			if (sorts == null || sorts.isEmpty()) {
				/* 如果不需要排序 */
			} else {
				/* 如果需要排序 */
				for (Sort sort : sorts) {
					SortOrder sortOrder = sort.getOrder().compareTo(Sort.SortOrder.DESC) == 0 ? SortOrder.DESC
							: SortOrder.ASC;

					searchRequestBuilder = searchRequestBuilder.addSort(sort.getSortBy().toLowerCase(), sortOrder);
				}
			}
			searchRequestBuilder.setExplain(false).setHighlighterRequireFieldMatch(true);
			// 此处加上聚合内容
			for (AggField aggField : aggFields) {
				// 先建第一级
				TermsBuilder termBuilder = AggregationBuilders.terms(aggField.getField() + "_aggs")
						.field(aggField.getField() + ".raw").size(0);
				// 循环创建子聚合
				termBuilder = SearchHelper.addSubAggs(termBuilder, aggField.getSubAggs());
				searchRequestBuilder.addAggregation(termBuilder);
			}
			SearchResponse response = searchRequestBuilder.get();

			List<T> list = SearchHelper.getSearchResult(response, clazz);

			result.setContents(list);
			result.setCounts(response.getHits().totalHits());
			result.setAggs(SearchHelper.getAgg(response, aggFields));
			result.setResultCode(PaaSConstant.RPC_CALL_OK);
		} catch (Exception e) {
			this.logger.error(e.getMessage(), e);
			throw new SearchRuntimeException("ES searchIndex error", e);
		}
		return result;
	}

	@Override
	public <T> T getById(String id, Class<T> clazz) {
		GetResponse response = client.prepareGet(indexName, indexName, id).get();
		if (null != response && response.isExists()) {
			Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ").create();
			return gson.fromJson(response.getSourceAsString(), clazz);
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
				+ "               \"filter\": [" + "                  \"lowercase\","
				+ "                  \"nGram_filter\"" + "               ]" + "            }" + "         }" + "      }"
				+ "   " + "}";
		CreateIndexResponse createResponse = client.admin().indices().prepareCreate(indexName).setSettings(setting)
				.get();
		if (createResponse.isAcknowledged()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean deleteIndex(String indexName) {
		DeleteIndexResponse delete;
		try {
			delete = client.admin().indices().delete(new DeleteIndexRequest(indexName)).get();
			if (!delete.isAcknowledged()) {
				logger.error("Index wasn't deleted!index=" + indexName);
				return false;
			}
			return true;
		} catch (Exception e) {
			logger.error(indexName, e);
			throw new SearchRuntimeException("ES delete index error", e);
		}
	}

	@Override
	public boolean addMapping(String indexName, String type, String json) {
		// 这里要做些处理，如果用户没有type,或者对应不上应该报错
		Assert.notNull(indexName, "Index Name can not null");
		Assert.notNull(type, "type can not null");
		Assert.notNull(json, "mapping can not null");
		// 转换成json看看
		JsonObject typeObj = null;
		Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ssZZZ").create();
		JsonObject jsonObj = gson.fromJson(json, JsonObject.class);
		if (null == jsonObj.get(type)) {
			// 看看有没有properties
			if (null == jsonObj.get("properties")) {
				// 这里好办了,补上两层
				JsonObject properties = new JsonObject();
				properties.add("properties", jsonObj);
				typeObj = new JsonObject();
				typeObj.add(type, properties);
			} else {
				// 这里也好办了,补上一层
				typeObj = new JsonObject();
				typeObj.add(type, jsonObj);
			}
		} else {
			// 存在就看自己是否正确构造
			typeObj = jsonObj;
		}

		PutMappingResponse putMappingResponse = client.admin().indices().preparePutMapping(indexName).setType(type)
				.setSource(gson.toJson(typeObj)).get();
		gson = null;
		if (putMappingResponse.isAcknowledged())
			return true;
		else
			return false;
	}

	@Override
	public boolean existIndex(String indexName) {
		Assert.notNull(indexName, "Index Name can not null");
		try {
			IndicesExistsResponse response = client.admin().indices().exists(new IndicesExistsRequest(indexName)).get();
			if (null != response && response.isExists())
				return true;
			else
				return false;
		} catch (Exception e) {
			logger.error(indexName, e);
			throw new SearchRuntimeException("ES delete index error", e);
		}
	}

	@Override
	public boolean refresh() {
		RefreshResponse response = client.admin().indices().prepareRefresh(indexName).get();
		if (null != response && response.getFailedShards() <= 0)
			return true;
		return false;
	}

	@Override
	public String searchBySQL(String querySQL, int from, int offset, List<Sort> sorts) {
		Gson gson = new Gson();
		return gson.toJson(searchBySQL(querySQL, from, offset, sorts, String.class));
	}

	@Override
	public String search(List<SearchCriteria> searchCriterias, int from, int offset, List<Sort> sorts) {
		Gson gson = new Gson();
		return gson.toJson(search(searchCriterias, from, offset, sorts, String.class));
	}

	@Override
	public String searchByDSL(String dslJson, int from, int offset, List<Sort> sorts) {
		Gson gson = new Gson();
		return gson.toJson(searchByDSL(dslJson, from, offset, sorts, String.class));
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
			this._id = id;
		return addMapping(indexName, type, json);
	}

	@Override
	public boolean createIndex(String indexName, String settings) {
		CreateIndexResponse createResponse = client.admin().indices().prepareCreate(indexName).setSettings(settings)
				.get();
		if (createResponse.isAcknowledged()) {
			return true;
		}
		return false;
	}

	@Override
	public boolean existMapping(String indexName, String mapping) {
		GetMappingsResponse response = client.admin().indices().prepareGetMappings(indexName).get();
		if (null != response) {
			return response.getMappings().containsKey(mapping);
		}
		return false;
	}

}
