package com.ai.paas.ipaas.search;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.search.impl.SearchClientImpl;
import com.ai.paas.util.Assert;

public class SearchCmpClientFactory {
    private static final Logger log = LoggerFactory.getLogger(SearchCmpClientFactory.class);
    private static Map<String, ISearchClient> searchClients = new ConcurrentHashMap<>();

    private SearchCmpClientFactory() {

    }

    /**
     * 
     * @param hosts   索引集群名称，形式为：10.1.234.5:9300,10.1.234.6:9300,10.1.234.7:9300，
     *                端口号为TCP端口号
     * @param indexId 索引名称
     * @param id      模型的主键字段
     * @return 搜索对象
     * @throws Exception
     */
    public static ISearchClient getSearchClient(String hosts, String indexId, String id) {
        ISearchClient searchClient = null;
        Assert.notNull(hosts, "ES hosts is null!");
        Assert.notNull(indexId, "indexId is null!");
        Assert.notNull(id, "mapping id is null!");
        log.info("ElasticSearch info is Hosts:{},index:{},id:{}", hosts, indexId, id);
        String plainHosts = hosts.replaceAll("\\.", "");
        plainHosts = plainHosts.replace(",", "");
        plainHosts = plainHosts.replace("\\:", "");
        log.info("ElasticSearch client key is {}.", plainHosts + "_" + indexId);
        if (searchClients.containsKey(plainHosts + "_" + indexId)) {
            searchClient = searchClients.get(plainHosts + "_" + indexId);
            // 这里要做个重新初始化
            ((SearchClientImpl) searchClient).initClient();
            return searchClient;
        }

        searchClient = new SearchClientImpl(hosts, indexId, id);
        searchClients.put(plainHosts + "_" + indexId, searchClient);
        return searchClient;
    }

    public static ISearchClient getSearchClient(String hosts, String indexId, String id, String userName,
            String passwd) {
        ISearchClient searchClient = null;
        Assert.notNull(hosts, "ES hosts is null!");
        Assert.notNull(indexId, "indexId is null!");
        Assert.notNull(id, "mapping id is null!");
        Assert.notNull(userName, "user name is null!");
        Assert.notNull(passwd, "passwd is null!");
        log.info("ElasticSearch info is Hosts:{},index:{},id:{}", hosts, indexId, id);
        String plainHosts = hosts.replaceAll("\\.", "");
        plainHosts = plainHosts.replace(",", "");
        plainHosts = plainHosts.replace("\\:", "");
        log.info("ElasticSearch client key is {}.", plainHosts + "_" + indexId);
        if (searchClients.containsKey(plainHosts + "_" + indexId)) {
            searchClient = searchClients.get(plainHosts + "_" + indexId);
            // 这里要做个重新初始化
            ((SearchClientImpl) searchClient).initClient();
            return searchClient;
        }

        searchClient = new SearchClientImpl(hosts, indexId, id, userName, passwd);
        searchClients.put(plainHosts + "_" + indexId, searchClient);
        return searchClient;
    }

}
