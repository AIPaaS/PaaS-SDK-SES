package com.ai.paas.ipaas.search.service;
//接口定义

import com.ai.paas.ipaas.search.vo.Results;
import com.ai.paas.ipaas.search.vo.SearchOption.SearchLogic;
import com.ai.paas.ipaas.search.vo.SearchVo;
import com.ai.paas.ipaas.search.vo.SearchfieldVo;
import com.ai.paas.ipaas.search.vo.SortField;
import org.elasticsearch.common.Nullable;

import java.util.List;
import java.util.Map;

/*new Object[] { "TESTNAME1,TESTNAME2"}会识别为一个搜索条件*/
/*new Object[] { "TESTNAME1","TESTNAME2"}会识别为两个搜索条件*/
public interface ISearchClient {


    //删除数据，危险
    boolean deleteData(List<SearchfieldVo> fieldList);

    boolean deleteData(String index, String type, String id);

//    boolean insertData(List<InsertFieldVo> fieldList);

    boolean cleanData();


    /**
     * 插入数据
     *
     * @param jsondata
     * @return
     */
    boolean insertData(String jsondata);

    boolean bulkInsertData(List<String> jsonData);


//    boolean bulkInsertData(List<List<InsertFieldVo>> dataList);

    // 更新数据，先删除，再插入，需要传递新数据的完整数据
    boolean updateData(List<SearchfieldVo> delFiledList, List<String> updateFiledList);


    boolean updateData(List<String> updateFiledList);

    /*
     * 搜索
     * ISearchClient.java   搜索字段内容
     * from    从第几条记录开始（必须大于等于0）
     * offset    一共显示多少条记录（必须大于0）
     * sortField    排序字段名称
     * sortType    排序方式（asc，desc）
     * */
    Results<Map<String, Object>> searchIndex(List<SearchfieldVo> fieldList, int from,
                                             int offset, SearchLogic logic, @Nullable String sortField, @Nullable String sortType);


    Results<Map<String, Object>> complexSearch(List<SearchVo> fieldList, int from,
                                               int offset, SearchLogic logic, @Nullable String sortField, @Nullable String sortType);


    Results<Map<String, Object>> simpleSearch(byte[] queryString
            , int from, int offset, @Nullable String sortField, @Nullable String sortType);


    //获得推荐列表
    List<String> getSuggest(String fieldName, String value, int count);//                                        @Nullable String soreType);


    Results<Map<String, Object>> search(List<SearchfieldVo> searchfieldVos, int from, int offset,
                                        @Nullable List<SortField> sortField);

    Results<Map<String, Long>> simpleAggregation(List<SearchfieldVo> searchfieldVos, String sortFields);
}