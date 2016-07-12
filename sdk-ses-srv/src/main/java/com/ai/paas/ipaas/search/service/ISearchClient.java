package com.ai.paas.ipaas.search.service;

//接口定义

import com.ai.paas.ipaas.search.common.JsonBuilder;
import com.ai.paas.ipaas.search.vo.AggField;
import com.ai.paas.ipaas.search.vo.AggResult;
import com.ai.paas.ipaas.search.vo.Result;
import com.ai.paas.ipaas.search.vo.SearchCriteria;
import com.ai.paas.ipaas.search.vo.Sort;

import org.elasticsearch.common.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ISearchClient {

	/**
	 * 插入单条索引数据
	 * 
	 * @param data
	 *            文档字段列表
	 * @return
	 */
	public boolean insert(Map<String, Object> data);

	/**
	 * 插入单条json数据
	 *
	 * @param json
	 * @return
	 */
	public boolean insert(String json);

	/**
	 * 通过泛型类插入单条数据，必须序列化泛型类
	 * 
	 * @param data
	 * @return
	 */
	public <T> boolean insert(T data);

	/**
	 * 通过builder插入单条数据
	 * 
	 * @param jsonBuilder
	 * @return
	 */
	public boolean insert(JsonBuilder jsonBuilder);

	/**
	 * 根据索引标识删除
	 * 
	 * @param id
	 * @return
	 */
	public boolean delete(String id);

	/**
	 * 删除多条数据
	 * 
	 * @param ids
	 * @return
	 */
	public boolean bulkDelete(List<String> ids);

	/**
	 * 根据查询条件删除多条数据
	 * 
	 * @param searchCriteria
	 *            查询条件
	 * @return
	 */
	public boolean delete(List<SearchCriteria> searchCriteria);

	/**
	 * 全部清空，危险操作
	 * 
	 * @return
	 */
	public boolean clean();

	/**
	 * 更新单个文档，合并模式
	 * 
	 * @param id
	 * @param data
	 * @return
	 */
	public boolean update(String id, Map<String, Object> data);

	/**
	 * 更新单个文档，合并模式
	 * 
	 * @param id
	 * @param json
	 * @return
	 */
	public boolean update(String id, String json);

	/**
	 * 更新单个文档，合并模式
	 * 
	 * @param id
	 * @param data
	 * @return
	 */
	public <T> boolean update(String id, T data);

	/**
	 * 更新单个文档，合并模式
	 * 
	 * @param id
	 * @param jsonBuilder
	 * @return
	 */
	public boolean update(String id, JsonBuilder jsonBuilder);

	/**
	 * 更新单个文档，不存在就插入
	 * 
	 * @param id
	 * @param data
	 * @return
	 */
	public boolean upsert(String id, Map<String, Object> data);

	/**
	 * 更新单个文档，不存在就插入
	 * 
	 * @param id
	 * @param json
	 * @return
	 */
	public boolean upsert(String id, String json);

	/**
	 * 更新单个文档，不存在就插入
	 * 
	 * @param id
	 * @param data
	 * @return
	 */
	public <T> boolean upsert(String id, T data);

	/**
	 * 更新单个文档，不存在就插入
	 * 
	 * @param id
	 * @param jsonBuilder
	 * @return
	 */
	public boolean upsert(String id, JsonBuilder jsonBuilder);

	/**
	 * 插入多条数据，数据为Map格式
	 * 
	 * @param datas
	 * @return
	 */
	public boolean bulkMapInsert(List<Map<String, Object>> datas);

	/**
	 * 插入多条数据，数据为json格式
	 *
	 * @param json
	 * @return
	 */
	public boolean bulkJsonInsert(List<String> jsons);

	/**
	 * 通过泛型类插入多条数据，必须序列化泛型类，数据类型所有字段都会插入
	 * 
	 * @param data
	 * @return
	 */
	public <T> boolean bulkInsert(List<T> datas);

	/**
	 * 通过builder插入多条数据
	 * 
	 * @param jsonBuilder
	 * @return
	 */
	public boolean bulkInsert(Set<JsonBuilder> jsonBuilders);

	/**
	 * 更新多个文档，合并模式
	 * 
	 * @param id
	 * @param data
	 * @return
	 */
	public boolean bulkMapUpdate(List<String> ids,
			List<Map<String, Object>> datas);

	/**
	 * 更新多个文档，合并模式
	 * 
	 * @param id
	 * @param json
	 * @return
	 */
	public boolean bulkJsonUpdate(List<String> ids, List<String> jsons);

	/**
	 * 更新多个文档，合并模式
	 * 
	 * @param id
	 * @param data
	 * @return
	 */
	public <T> boolean bulkUpdate(List<String> ids, List<T> datas);

	/**
	 * 更新多个文档，合并模式
	 * 
	 * @param id
	 * @param jsonBuilder
	 * @return
	 */
	public boolean bulkUpdate(List<String> ids, Set<JsonBuilder> jsonBuilders);

	/**
	 * 更新多个文档，不存在就插入
	 * 
	 * @param id
	 * @param data
	 * @return
	 */
	public boolean bulkMapUpsert(List<String> ids,
			List<Map<String, Object>> datas);

	/**
	 * 更新多个文档，不存在就插入
	 * 
	 * @param id
	 * @param json
	 * @return
	 */
	public boolean bulkJsonUpsert(List<String> ids, List<String> jsons);

	/**
	 * 更新多个文档，不存在就插入
	 * 
	 * @param id
	 * @param data
	 * @return
	 */
	public <T> boolean bulkUpsert(List<String> ids, List<T> datas);

	/**
	 * 更新多个文档，不存在就插入
	 * 
	 * @param id
	 * @param jsonBuilder
	 * @return
	 */
	public boolean bulkUpsert(List<String> ids, Set<JsonBuilder> jsonBuilders);

	/**
	 * 按照条件查询，age:(>=10 AND <50) | age:(>=10 AND <50) AND name:1234
	 * 具体语法请参见：https
	 * ://www.elastic.co/guide/en/elasticsearch/reference/current/query
	 * -dsl-query-string-query.html
	 * 
	 * @param query
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @return
	 */
	public <T> Result<T> searchBySQL(String querySQL, int from, int offset,
			@Nullable List<Sort> sorts, Class<T> clazz);

	/**
	 * 按照条件查询，age:(>=10 AND <50) | age:(>=10 AND <50) AND name:1234
	 * 具体语法请参见：https
	 * ://www.elastic.co/guide/en/elasticsearch/reference/current/query
	 * -dsl-query-string-query.html
	 * 
	 * @param query
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @return json格式的结果
	 */
	public String searchBySQL(String querySQL, int from, int offset,
			@Nullable List<Sort> sorts);

	/**
	 * 
	 * @param searchCriterias
	 * @param from
	 * @param offset
	 * @param logic
	 * @param sorts
	 * @param clazz
	 * @return
	 */
	public <T> Result<T> search(List<SearchCriteria> searchCriterias, int from,
			int offset, @Nullable List<Sort> sorts, Class<T> clazz);

	/**
	 * @param searchCriterias
	 * @param from
	 * @param offset
	 * @param sorts
	 * @return
	 */
	public String search(List<SearchCriteria> searchCriterias, int from,
			int offset, @Nullable List<Sort> sorts);

	/**
	 * DSL格式的查询
	 * 
	 * { "query": { "bool": { "must": [ { "match": { "name": "开发"}}, { "match":
	 * { "age": 51 }} ], "filter": [ { "term": { "userId": "107" }}, { "range":
	 * { "created": { "gte": "2016-06-20" }}} ] } } }
	 * 
	 * @param dslJson
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @return
	 */
	public <T> Result<T> searchByDSL(String dslJson, int from, int offset,
			@Nullable List<Sort> sorts, Class<T> clazz);

	/**
	 * DSL格式的查询
	 * 
	 * { "query": { "bool": { "must": [ { "match": { "name": "开发"}}, { "match":
	 * { "age": 51 }} ], "filter": [ { "term": { "userId": "107" }}, { "range":
	 * { "created": { "gte": "2016-06-20" }}} ] } } }
	 * 
	 * @param dslJson
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @return
	 */
	public String searchByDSL(String dslJson, int from, int offset,
			@Nullable List<Sort> sorts);

	/**
	 * 全文检索
	 * 
	 * @param text
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @return
	 */
	public <T> Result<T> fullTextSearch(String text, int from, int offset,
			@Nullable List<Sort> sorts, Class<T> clazz);

	/**
	 * 全文检索
	 * 如果带聚合必须指定对哪些字段进行全文索引，各字段是或者关系
	 * @param text
	 * @param qryFields 全文索引字段
	 * @param aggFields 聚合字段，如品牌、价格、类型等
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @return
	 */
	public <T> Result<T> fullTextSearch(String text, List<String> qryFields,
			List<AggField> aggFields, int from, int offset,
			@Nullable List<Sort> sorts, Class<T> clazz);

	/**
	 * 根据id获取文档，
	 * 
	 * @param id
	 *            文档标识
	 * @param clazz
	 *            返回的类型
	 * @return
	 */
	public <T> T getById(String id, Class<T> clazz);

	/**
	 * 根据id获取文档
	 * 
	 * @param id
	 * @return json格式的文档
	 */
	public String getById(String id);

	/**
	 * 获得搜索提示 服务器端安装elasticsearch-plugin-suggest
	 * 客户端加入elasticsearch-plugin-suggest的jar包
	 * https://github.com/spinscale/elasticsearch-suggest-plugin
	 * 
	 * @param fieldName
	 * @param value
	 * @param count
	 * @return
	 * 
	 */
	public List<String> getSuggest(String field, String value, int count);

	/**
	 * 全局检索提示
	 * 
	 * @param value
	 * @param count
	 * @return
	 */
	public List<String> getSuggest(String value, int count);

	/**
	 * 根据查询条件进行某个字段的聚合
	 * 
	 * @param searchCriterias
	 * @param field
	 * @param clazz
	 * @return
	 */
	public Result<Map<String, Long>> aggregate(
			List<SearchCriteria> searchCriterias, String field);

	/**
	 * 根据查询条件进行多个字段的聚合
	 * 
	 * @param searchCriterias
	 * @param fields
	 * @return
	 */
	public Result<List<AggResult>> aggregate(
			List<SearchCriteria> searchCriterias, List<AggField> fields);

	/**
	 * 创建索引
	 * 
	 * @param indexName
	 * @return
	 */
	public boolean createIndex(String indexName, int shards, int replicas);

	/**
	 * 删除索引
	 * 
	 * @param indexName
	 * @return
	 */
	public boolean deleteIndex(String indexName);

	/**
	 * 索引是否存在
	 * 
	 * @param indexName
	 * @return
	 */
	public boolean existIndex(String indexName);

	/**
	 * 增加索引对象定义，自从2.0以后，不支持在设置ID为文档的某个字段，需要在 插入或获取时自己指定
	 * 
	 * @param indexName
	 *            "user"
	 * @param type
	 *            "userInfo"
	 * @param json
	 *            <pre>
	 * {
	 *   "userInfo" : {
	 *     "properties" : {
	 *     	 "userId" :  {"type" : "string", "store" : "yes","index": "not_analyzed"}
	 *       "message" : {"type" : "string", "store" : "yes"}
	 *     }
	 *   }
	 * }
	 * </pre>
	 * @return
	 */
	public boolean addMapping(String indexName, String type, String json);

	/**
	 * 刷新插入或者更新
	 * 
	 * @return
	 */
	public boolean refresh();

}