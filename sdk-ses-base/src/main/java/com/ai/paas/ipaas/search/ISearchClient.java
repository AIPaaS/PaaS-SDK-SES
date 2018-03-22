package com.ai.paas.ipaas.search;

import com.ai.paas.ipaas.search.common.DynamicMatchOption;

//接口定义

import com.ai.paas.ipaas.search.common.JsonBuilder;
import com.ai.paas.ipaas.search.common.TypeGetter;
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
	 * @return 插入数据是否成功
	 */
	public boolean insert(Map<String, Object> data);

	/**
	 * 插入单条json数据
	 *
	 * @param json
	 * @return 插入数据是否成功
	 */
	public boolean insert(String json);

	/**
	 * 通过泛型类插入单条数据，必须序列化泛型类
	 * 
	 * @param data
	 * @return 插入数据是否成功
	 */
	public <T> boolean insert(T data);

	/**
	 * 通过builder插入单条数据
	 * 
	 * @param jsonBuilder
	 * @return 插入数据是否成功
	 */
	public boolean insert(JsonBuilder jsonBuilder);

	/**
	 * 根据索引标识删除
	 * 
	 * @param id
	 * @return 删除文档，返回是否成功
	 */
	public boolean delete(String id);

	/**
	 * 删除多条数据
	 * 
	 * @param ids
	 * @return 是否删除成功，所有删除成功则成功，否则失败
	 */
	public boolean bulkDelete(List<String> ids);

	/**
	 * 删除多条数据
	 * 
	 * @param ids
	 * @param rebuildIndex
	 *            是否重建索引，重建索引会引起性能问题
	 * @return 是否删除成功，所有删除成功则成功，否则失败
	 */
	public boolean bulkDelete(List<String> ids, boolean rebuildIndex);

	/**
	 * 根据查询条件删除多条数据
	 * 
	 * @param searchCriteria
	 *            查询条件
	 * @return 是否删除成功，所有删除成功则成功，否则失败
	 */
	public boolean delete(List<SearchCriteria> searchCriteria);

	/**
	 * 根据查询条件删除多条数据
	 * 
	 * @param searchCriteria
	 *            查询条件
	 * @param rebuildIndex
	 *            是否重建索引，重建索引会引起性能问题
	 * @return 是否删除成功，所有删除成功则成功，否则失败
	 */
	public boolean delete(List<SearchCriteria> searchCriteria, boolean rebuildIndex);

	/**
	 * 全部清空，危险操作
	 * 
	 * @return 清除所有数据
	 */
	public boolean clean();

	/**
	 * 更新单个文档，合并模式
	 * 
	 * @param id
	 * @param data
	 * @return 更新文档是否成功
	 */
	public boolean update(String id, Map<String, Object> data);

	/**
	 * 更新单个文档，合并模式
	 * 
	 * @param id
	 * @param json
	 * @return 更新文档是否成功
	 */
	public boolean update(String id, String json);

	/**
	 * 更新单个文档，合并模式
	 * 
	 * @param id
	 * @param data
	 * @return 更新文档是否成功
	 */
	public <T> boolean update(String id, T data);

	/**
	 * 更新单个文档，合并模式
	 * 
	 * @param id
	 * @param jsonBuilder
	 * @return 更新文档是否成功
	 */
	public boolean update(String id, JsonBuilder jsonBuilder);

	/**
	 * 更新单个文档，不存在就插入
	 * 
	 * @param id
	 * @param data
	 * @return 更新文档是否成功
	 */
	public boolean upsert(String id, Map<String, Object> data);

	/**
	 * 更新单个文档，不存在就插入
	 * 
	 * @param id
	 * @param json
	 * @return 更新插入文档是否成功
	 */
	public boolean upsert(String id, String json);

	/**
	 * 更新单个文档，不存在就插入
	 * 
	 * @param id
	 * @param data
	 * @return 更新插入文档是否成功
	 */
	public <T> boolean upsert(String id, T data);

	/**
	 * 更新单个文档，不存在就插入
	 * 
	 * @param id
	 * @param jsonBuilder
	 * @return 更新插入文档是否成功
	 */
	public boolean upsert(String id, JsonBuilder jsonBuilder);

	/**
	 * 插入多条数据，数据为Map格式
	 * 
	 * @param datas
	 * @return 插入是否成功,如果有一条插入错误，则返回否
	 */
	public boolean bulkMapInsert(List<Map<String, Object>> datas);

	/**
	 * 插入多条数据，数据为Map格式
	 * 
	 * @param datas
	 * @param rebuidIndex
	 *            是否重建索引，数据量大时设置为false
	 * @return 插入是否成功,如果有一条插入错误，则返回否
	 */
	public boolean bulkMapInsert(List<Map<String, Object>> datas, boolean rebuidIndex);

	/**
	 * 插入多条数据，数据为json格式
	 *
	 * @param jsons
	 * @return 插入是否成功,如果有一条插入错误，则返回否
	 */
	public boolean bulkJsonInsert(List<String> jsons);

	/**
	 * 插入多条数据，数据为json格式
	 * 
	 * @param jsons
	 * @param rebuidIndex
	 *            是否重建索引，数据量大时设置为false
	 * @return 插入是否成功,如果有一条插入错误，则返回否
	 */
	public boolean bulkJsonInsert(List<String> jsons, boolean rebuidIndex);

	/**
	 * 通过泛型类插入多条数据，必须序列化泛型类，数据类型所有字段都会插入
	 * 
	 * @param datas
	 * @return 插入是否成功,如果有一条插入错误，则返回否
	 */
	public <T> boolean bulkInsert(List<T> datas);

	/**
	 * 通过泛型类插入多条数据，必须序列化泛型类，数据类型所有字段都会插入
	 * 
	 * @param datas
	 * @param rebuidIndex
	 *            是否重建索引，数据量大时设置为false
	 * @return 插入是否成功,如果有一条插入错误，则返回否
	 */
	public <T> boolean bulkInsert(List<T> datas, boolean rebuidIndex);

	/**
	 * 通过builder插入多条数据
	 * 
	 * @param jsonBuilders
	 * @return 插入是否成功,如果有一条插入错误，则返回否
	 */
	public boolean bulkInsert(Set<JsonBuilder> jsonBuilders);

	/**
	 * 通过builder插入多条数据
	 * 
	 * @param jsonBuilders
	 * @param rebuidIndex
	 *            是否重建索引，数据量大时设置为false
	 * @return 插入是否成功,如果有一条插入错误，则返回否
	 */
	public boolean bulkInsert(Set<JsonBuilder> jsonBuilders, boolean rebuidIndex);

	/**
	 * 更新多个文档，合并模式
	 * 
	 * @param ids
	 * @param datas
	 * @return 更新是否成功,如果有一条更新失败，则返回失败
	 */
	public boolean bulkMapUpdate(List<String> ids, List<Map<String, Object>> datas);

	/**
	 * 更新多个文档，合并模式
	 * 
	 * @param ids
	 * @param datas
	 * @param rebuidIndex
	 *            是否重建索引，数据量大时设置为false
	 * @return 更新是否成功,如果有一条更新失败，则返回失败
	 */
	public boolean bulkMapUpdate(List<String> ids, List<Map<String, Object>> datas, boolean rebuidIndex);

	/**
	 * 更新多个文档，合并模式
	 * 
	 * @param ids
	 * @param jsons
	 * @return 更新是否成功,如果有一条更新失败，则返回失败
	 */
	public boolean bulkJsonUpdate(List<String> ids, List<String> jsons);

	/**
	 * 更新多个文档，合并模式
	 * 
	 * @param ids
	 * @param jsons
	 * @param rebuidIndex
	 *            是否重建索引，数据量大时设置为false
	 * @return 更新是否成功,如果有一条更新失败，则返回失败
	 */
	public boolean bulkJsonUpdate(List<String> ids, List<String> jsons, boolean rebuidIndex);

	/**
	 * 更新多个文档，合并模式
	 * 
	 * @param ids
	 * @param datas
	 * @return 更新是否成功,如果有一条更新失败，则返回失败
	 */
	public <T> boolean bulkUpdate(List<String> ids, List<T> datas);

	/**
	 * 更新多个文档，合并模式
	 * 
	 * @param ids
	 * @param datas
	 * @param rebuidIndex
	 *            是否重建索引，数据量大时设置为false
	 * @return 更新是否成功,如果有一条更新失败，则返回失败
	 */
	public <T> boolean bulkUpdate(List<String> ids, List<T> datas, boolean rebuidIndex);

	/**
	 * 更新多个文档，合并模式
	 * 
	 * @param ids
	 * @param jsonBuilders
	 * @return 更新是否成功,如果有一条更新失败，则返回失败
	 */
	public boolean bulkUpdate(List<String> ids, Set<JsonBuilder> jsonBuilders);

	/**
	 * 更新多个文档，合并模式
	 * 
	 * @param ids
	 * @param jsonBuilders
	 * @param rebuidIndex
	 *            是否重建索引，数据量大时设置为false
	 * @return 更新是否成功,如果有一条更新失败，则返回失败
	 */
	public boolean bulkUpdate(List<String> ids, Set<JsonBuilder> jsonBuilders, boolean rebuidIndex);

	/**
	 * 更新多个文档，不存在就插入
	 * 
	 * @param ids
	 * @param datas
	 * @return 更新是否成功,如果有一条更新失败，则返回失败
	 */
	public boolean bulkMapUpsert(List<String> ids, List<Map<String, Object>> datas);

	/**
	 * 更新多个文档，不存在就插入
	 * 
	 * @param ids
	 * @param datas
	 * @param rebuidIndex
	 *            是否重建索引，数据量大时设置为false
	 * @return 更新是否成功,如果有一条更新失败，则返回失败
	 */
	public boolean bulkMapUpsert(List<String> ids, List<Map<String, Object>> datas, boolean rebuidIndex);

	/**
	 * 更新多个文档，不存在就插入
	 * 
	 * @param ids
	 * @param jsons
	 * @return 更新是否成功,如果有一条更新失败，则返回失败
	 */
	public boolean bulkJsonUpsert(List<String> ids, List<String> jsons);

	/**
	 * 更新多个文档，不存在就插入
	 * 
	 * @param ids
	 * @param jsons
	 * @param rebuidIndex
	 *            是否重建索引，数据量大时设置为false
	 * @return 更新是否成功,如果有一条更新失败，则返回失败
	 */
	public boolean bulkJsonUpsert(List<String> ids, List<String> jsons, boolean rebuidIndex);

	/**
	 * 更新多个文档，不存在就插入
	 * 
	 * @param ids
	 * @param datas
	 * @return 批量更新插入是否成功，有一个错误则失败
	 */
	public <T> boolean bulkUpsert(List<String> ids, List<T> datas);
	
	/**
	 * 更新多个文档，不存在就插入
	 * @param ids
	 * @param datas
	 * @param rebuidIndex 是否重建索引，数据量大时设置为false
	 * @return 批量更新插入是否成功，有一个错误则失败
	 */
	public <T> boolean bulkUpsert(List<String> ids, List<T> datas, boolean rebuidIndex);

	/**
	 * 更新多个文档，不存在就插入
	 * 
	 * @param ids
	 * @param jsonBuilders
	 * @return 批量更新插入是否成功，有一个错误则失败
	 */
	public boolean bulkUpsert(List<String> ids, Set<JsonBuilder> jsonBuilders);
	
	/**
	 * 更新多个文档，不存在就插入
	 * @param ids
	 * @param jsonBuilders
	 * @param rebuidIndex 是否重建索引，数据量大时设置为false
	 * @return 批量更新插入是否成功，有一个错误则失败
	 */
	public boolean bulkUpsert(List<String> ids, Set<JsonBuilder> jsonBuilders, boolean rebuidIndex);

	/**
	 * 按照条件查询，age:(>=10 AND <50) | age:(>=10 AND <50) AND name:1234
	 * 查询结果受模型定义及分词定义影响 具体语法请参见：https
	 * ://www.elastic.co/guide/en/elasticsearch/reference/current/query
	 * -dsl-query-string-query.html
	 * 
	 * @param querySQL
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @return 查询结果对象
	 */
	public <T> Result<T> searchBySQL(String querySQL, int from, int offset, @Nullable List<Sort> sorts, Class<T> clazz);

	/**
	 * 按照条件查询，age:(>=10 AND <50) | age:(>=10 AND <50) AND name:1234
	 * 查询结果受模型定义及分词定义影响 具体语法请参见：https
	 * ://www.elastic.co/guide/en/elasticsearch/reference/current/query
	 * -dsl-query-string-query.html
	 * 
	 * @param querySQL
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @param resultFields
	 *            指定返回的结果字段
	 * @return
	 */
	public <T> Result<T> searchBySQL(String querySQL, int from, int offset, @Nullable List<Sort> sorts, Class<T> clazz,
			String[] resultFields);

	/**
	 * 按照条件查询，age:(>=10 AND <50) | age:(>=10 AND <50) AND name:1234
	 * 具体语法请参见：https
	 * ://www.elastic.co/guide/en/elasticsearch/reference/current/query
	 * -dsl-query-string-query.html
	 * 
	 * @param querySQL
	 * @param from
	 * @param offset
	 * @param sorts
	 * @return json格式的结果
	 */
	public String searchBySQL(String querySQL, int from, int offset, @Nullable List<Sort> sorts);

	/**
	 * 按照条件查询，age:(>=10 AND <50) | age:(>=10 AND <50) AND name:1234
	 * 具体语法请参见：https
	 * ://www.elastic.co/guide/en/elasticsearch/reference/current/query
	 * -dsl-query-string-query.html
	 * 
	 * @param querySQL
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param resultFields
	 *            指定返回的结果字段
	 * @return
	 */
	public String searchBySQL(String querySQL, int from, int offset, @Nullable List<Sort> sorts, String[] resultFields);

	/**
	 * 
	 * @param searchCriterias
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @return 查询结果对象
	 */
	public <T> Result<T> search(List<SearchCriteria> searchCriterias, int from, int offset, @Nullable List<Sort> sorts,
			Class<T> clazz);

	/**
	 * 按照查询对象进行查询，查询条件对象支持各种嵌套
	 * 
	 * @param searchCriterias
	 *            查询条件对象列表
	 * @param from
	 *            分页查询起始位置
	 * @param offset
	 *            分页每页数量
	 * @param sorts
	 *            排序对象列表，可NULL
	 * @param typeGetter
	 *            类型获取器，主要用来支持泛型类型，使用方式：TypeGetter<List<String>>
	 *            typeGetter=TypeGetter<List<String>>() {}; 其中里面为泛型的具体使用方式
	 * @return
	 */
	public <T> Result<T> search(List<SearchCriteria> searchCriterias, int from, int offset, @Nullable List<Sort> sorts,
			@SuppressWarnings("rawtypes") TypeGetter typeGetter);

	/**
	 * 按照查询条件分页查询
	 * 
	 * @param searchCriterias
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @param resultFields
	 *            指定返回的结果字段
	 * @return
	 */
	public <T> Result<T> search(List<SearchCriteria> searchCriterias, int from, int offset, @Nullable List<Sort> sorts,
			Class<T> clazz, String[] resultFields);

	/**
	 * 按照查询对象进行查询，查询条件对象支持各种嵌套
	 * 
	 * @param searchCriterias
	 *            查询条件对象列表
	 * @param from
	 *            分页查询起始位置
	 * @param offset
	 *            分页每页数量
	 * @param sorts
	 *            排序对象列表，可NULL
	 * @param typeGetter
	 *            类型获取器，主要用来支持泛型类型，使用方式：TypeGetter<List<String>>
	 *            typeGetter=TypeGetter<List<String>>() {}; 其中里面为泛型的具体使用方式
	 * @param resultFields
	 *            需要返回的列数组
	 * @return
	 */
	public <T> Result<T> search(List<SearchCriteria> searchCriterias, int from, int offset, @Nullable List<Sort> sorts,
			@SuppressWarnings("rawtypes") TypeGetter typeGetter, String[] resultFields);

	/**
	 * @param searchCriterias
	 * @param from
	 * @param offset
	 * @param sorts
	 * @return 返回json
	 */
	public String search(List<SearchCriteria> searchCriterias, int from, int offset, @Nullable List<Sort> sorts);

	/**
	 * 
	 * @param searchCriterias
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param resultFields
	 *            指定返回的结果字段
	 * @return
	 */
	public String search(List<SearchCriteria> searchCriterias, int from, int offset, @Nullable List<Sort> sorts,
			String[] resultFields);

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
	 * @return 查询结果对象
	 */
	public <T> Result<T> searchByDSL(String dslJson, int from, int offset, @Nullable List<Sort> sorts, Class<T> clazz);

	/**
	 * 
	 * @param dslJson
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @param resultFields
	 * @return
	 */
	public <T> Result<T> searchByDSL(String dslJson, int from, int offset, @Nullable List<Sort> sorts, Class<T> clazz,
			String[] resultFields);

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
	 * @return 返回json
	 */
	public String searchByDSL(String dslJson, int from, int offset, @Nullable List<Sort> sorts);

	/**
	 * 
	 * @param dslJson
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param resultFields
	 * @return
	 */
	public String searchByDSL(String dslJson, int from, int offset, @Nullable List<Sort> sorts, String[] resultFields);

	/**
	 * 全文检索
	 * 
	 * @param text
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @return 查询结果对象
	 */
	public <T> Result<T> fullTextSearch(String text, int from, int offset, @Nullable List<Sort> sorts, Class<T> clazz);

	/**
	 * 全文检索 指定对哪些字段进行全文索引，各字段是或者关系
	 * 
	 * @param text
	 * @param qryFields
	 *            全文索引字段
	 * @param aggFields
	 *            聚合字段，如品牌、价格、类型等
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @return 查询结果对象
	 */
	public <T> Result<T> fullTextSearch(String text, List<String> qryFields, List<AggField> aggFields, int from,
			int offset, @Nullable List<Sort> sorts, Class<T> clazz);

	/**
	 * 全文检索 对所有字段进行
	 * 
	 * @param text
	 * @param aggFields
	 * @param from
	 * @param offset
	 * @param sorts
	 * @param clazz
	 * @return 查询结果对象
	 */
	public <T> Result<T> fullTextSearch(String text, List<AggField> aggFields, int from, int offset,
			@Nullable List<Sort> sorts, Class<T> clazz);

	/**
	 * 根据id获取文档，
	 * 
	 * @param id
	 *            文档标识
	 * @param clazz
	 *            返回的类型
	 * @return 返回自定义类型文档
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
	 * 获得搜索提示
	 * 
	 * @param field
	 * @param value
	 * @param count
	 * @return 返回提示列表
	 * 
	 */
	public List<String> getSuggest(String field, String value, int count);

	/**
	 * 全局检索提示
	 * 
	 * @param value
	 * @param count
	 * @return 返回提示列表
	 */
	public List<String> getSuggest(String value, int count);

	/**
	 * 根据查询条件进行某个字段的聚合
	 * 
	 * @param searchCriterias
	 * @param field
	 * @return 返回结果对象，包含汇聚和结果
	 */
	public Result<Map<String, Long>> aggregate(List<SearchCriteria> searchCriterias, String field);

	/**
	 * 根据查询条件进行多个字段的聚合
	 * 
	 * @param searchCriterias
	 * @param fields
	 * @return 返回结果对象，包含汇聚和结果
	 */
	public Result<List<AggResult>> aggregate(List<SearchCriteria> searchCriterias, List<AggField> fields);

	/**
	 * 创建索引
	 * 
	 * @param indexName
	 * @return 是否创建索引成功
	 */
	public boolean createIndex(String indexName, int shards, int replicas);

	/**
	 * 创建索引，可以指定索引的设置
	 * 
	 * @param indexName
	 * @param settings
	 *            json格式的" {" + "\"number_of_shards\":\""5\"," +
	 *            "\"number_of_replicas\":\"2\"," +
	 *            "\"client.transport.ping_timeout\":\"60s\"," + " \"analysis\":
	 *            {" + " \"filter\": {" + " \"nGram_filter\": {" + " \"type\":
	 *            \"nGram\"," + " \"min_gram\": 1," + " \"max_gram\": 10" + " }"
	 *            + " }," + " \"analyzer\": {" + " \"nGram_analyzer\": {" + "
	 *            \"type\": \"custom\"," + " \"tokenizer\": \"ik_max_word\"," +
	 *            " \"filter\": [" + " \"lowercase\"," + " \"nGram_filter\"" + "
	 *            ]" + " }" + " }" + " }" + " " + "}"
	 * @return 是否创建索引成功
	 */
	public boolean createIndex(String indexName, String settings);

	/**
	 * 删除索引
	 * 
	 * @param indexName
	 * @return 是否删除索引成功
	 */
	public boolean deleteIndex(String indexName);

	/**
	 * 索引是否存在
	 * 
	 * @param indexName
	 * @return 索引是否存在
	 */
	public boolean existIndex(String indexName);

	/**
	 * 判断索引模型是否存在
	 * 
	 * @param indexName
	 * @param mapping
	 * @return
	 */
	public boolean existMapping(String indexName, String mapping);

	/**
	 * 增加索引对象定义，自从2.0以后，不支持在设置ID为文档的某个字段，需要在 插入或获取时自己指定
	 * 
	 * @param indexName
	 *            "user"
	 * @param type
	 *            "userInfo"
	 * @param json
	 * 
	 *            <pre>
	 * {
	 *   "userInfo" : {
	 *     "properties" : {
	 *     	 "userId" :  {"type" : "string", "store" : "yes","index": "not_analyzed"}
	 *       "message" : {"type" : "string", "store" : "yes"}
	 *     }
	 *   }
	 * }
	 *            </pre>
	 * 
	 * @return 增加模型是否成功
	 */
	public boolean addMapping(String indexName, String type, String json);

	/**
	 * 增加索引对象定义，自从2.0以后，不支持在设置ID为文档的某个字段，需要在 插入或获取时自己指定
	 * 
	 * @param indexName
	 *            "user"
	 * @param type
	 *            "userInfo"
	 * @param json
	 * 
	 * @param addDynamicTemplate
	 *            是否增加动态模板
	 * 
	 *            <pre>
	 * {
	 *   "userInfo" : {
	 *     "properties" : {
	 *     	 "userId" :  {"type" : "string", "store" : "yes","index": "not_analyzed"}
	 *       "message" : {"type" : "string", "store" : "yes"}
	 *     }
	 *   }
	 * }
	 *            </pre>
	 * 
	 * @return 增加模型是否成功
	 */
	public boolean addMapping(String indexName, String type, String json, boolean addDynamicTemplate);

	/**
	 * 动态模板的mapping创建，支持指定那些值得进行分词，哪些不进行分词，由matchs控制，注意顺序，前面的匹配就进行前面的规则了，一定要配置一条默认规则
	 * 
	 * @param indexName
	 * @param type
	 * @param json
	 * @param matchs
	 * @return
	 */
	public boolean addMapping(String indexName, String type, String json, List<DynamicMatchOption> matchs);

	/**
	 * 按照指定的主键和设置创建索引
	 * 
	 * @param indexName
	 *            索引名称
	 * @param type
	 *            模型名称
	 * @param json
	 *            模型定义
	 * @param id
	 *            主键字段
	 * @return 增加模型是否成功
	 */
	public boolean addMapping(String indexName, String type, String json, String id);

	/**
	 * 刷新插入或者更新
	 * 
	 * @return 刷新是否成功，文档提交等操作生效
	 */
	public boolean refresh();

	/**
	 * 设置索引重建索引的时间， 
	 * @param seconds 秒数，-1表示不刷新
	 */
	public boolean setRefeshTime(long seconds);
	/**
	 * 关闭
	 */
	public void close();
}
