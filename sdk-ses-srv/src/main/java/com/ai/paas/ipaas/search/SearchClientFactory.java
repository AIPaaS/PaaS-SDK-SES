package com.ai.paas.ipaas.search;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.ccs.inner.CCSComponentFactory;
import com.ai.paas.ipaas.ccs.inner.ICCSComponent;
import com.ai.paas.ipaas.ccs.zookeeper.ConfigWatcher;
import com.ai.paas.ipaas.ccs.zookeeper.ConfigWatcher.Event.EventType;
import com.ai.paas.ipaas.ccs.zookeeper.ConfigWatcherEvent;
import com.ai.paas.ipaas.search.impl.SearchClientImpl;
import com.ai.paas.ipaas.uac.service.UserClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
import com.ai.paas.util.Assert;
import com.ai.paas.util.StringUtil;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class SearchClientFactory {
    private static  final Logger log = LoggerFactory.getLogger(SearchClientFactory.class);
    private static Map<String, ISearchClient> searchClients = new ConcurrentHashMap<String, ISearchClient>();
    private static final String SEARCH_CONFIG_PATH = "/SES/";
    private static final String SES_MAPPING_ZK_PATH = "/SES/MAPPING/";
    private static final String ELASTIC_HOST = "hosts";
    @SuppressWarnings("unused")
    private final static String ELASTIC_MAPPING = "mapping";

    private SearchClientFactory() {

    }

    @SuppressWarnings({ "unchecked" })
    public static ISearchClient getSearchClient(AuthDescriptor ad) throws Exception {
        ISearchClient searchClient = null;
        log.info("Check Formal Parameter AuthDescriptor ...");
        Assert.notNull(ad, "AuthDescriptor对象为空");
        Assert.notNull(ad.getServiceId(), "service_id为空");
        String srvId = ad.getServiceId();
        String userPid = ad.getPid();
        if (searchClients.containsKey(userPid + "_" + srvId)) {
            searchClient = searchClients.get(userPid + "_" + srvId);
            // 这里要做个重新初始化
            ((SearchClientImpl) searchClient).initClient();
            return searchClient;
        }
        AuthResult authResult = UserClientFactory.getUserClient().auth(ad);
        Assert.notNull(authResult.getUserId(), "UserId为空");
        // // 开始初始化
        Assert.notNull(authResult.getConfigAddr(), "ConfigAddr为空");
        Assert.notNull(authResult.getConfigUser(), "ConfigUser为空");
        Assert.notNull(authResult.getConfigPasswd(), "ConfigPasswd为空");
        // // 获取内部zk地址后取得该用户的cache配置信息，返回JSON String
        // // 获取该用户申请的cache服务配置信息
        log.info("Get confBase&conf ...");
        String userId = authResult.getUserId();
        ICCSComponent client = CCSComponentFactory.getConfigClient(authResult.getConfigAddr(),
                authResult.getConfigUser(), authResult.getConfigPasswd());

        SesWatch watch = new SesWatch(client, userPid, userId, srvId);

        String personalConf = CCSComponentFactory
                .getConfigClient(authResult.getConfigAddr(), authResult.getConfigUser(), authResult.getConfigPasswd())
                .get(SEARCH_CONFIG_PATH + srvId);

        String mappingConf = CCSComponentFactory
                .getConfigClient(authResult.getConfigAddr(), authResult.getConfigUser(), authResult.getConfigPasswd())
                .get(SES_MAPPING_ZK_PATH + srvId, watch);

        Gson gson = new Gson();
        Map<String, Object> personalConfMap = new HashMap<String, Object>();
        Map<String, Object> mappingConfMap = new HashMap<String, Object>();
        personalConfMap = gson.fromJson(personalConf, personalConfMap.getClass());

        mappingConfMap = gson.fromJson(mappingConf, mappingConfMap.getClass());
        String indexName = String.valueOf(Math.abs((authResult.getUserId() + srvId).hashCode()));
        String hosts = String.valueOf(personalConfMap.get(ELASTIC_HOST));

        // 这里得改变了
        String idConf = CCSComponentFactory
                .getConfigClient(authResult.getConfigAddr(), authResult.getConfigUser(), authResult.getConfigPasswd())
                .get(SEARCH_CONFIG_PATH + srvId + "/indexPK", watch);
        String id = null;
        if (!StringUtil.isBlank(idConf)) {
            JsonObject idObj = gson.fromJson(idConf, JsonObject.class);
            id = null != idObj.get("indexPK") ? idObj.get("indexPK").getAsString() : null;
        }
        searchClient = new SearchClientImpl(hosts, indexName, id);
        searchClients.put(userPid + "_" + srvId, searchClient);
        return searchClient;
    }

    private static class SesWatch extends ConfigWatcher {
        private ICCSComponent client;
        private String userPid;
        private String userId;
        private String serviceId;

        public SesWatch(ICCSComponent client, String userPid, String userId, String serviceId) {
            this.client = client;
            this.serviceId = serviceId;
            this.userPid = userPid;
            this.userId = userId;
        }

        @SuppressWarnings({ "unchecked" })
        @Override
        public void processEvent(ConfigWatcherEvent event) {
            if (event == null)
                return;
            // 事件类型
            EventType eventType = event.getType();
            if (EventType.NodeDataChanged == eventType) {
                log.info("------monitor--投票结果------NodeDataChanged--------");
                try {
                    // 循环watch

                    ISearchClient iSearchClient = null;

                    String personalConf = client.get(SEARCH_CONFIG_PATH + serviceId, this);
                    String mappingConf = client.get(SES_MAPPING_ZK_PATH + serviceId, this);

                    Gson gson = new Gson();
                    Map<String, Object> personalConfMap = new HashMap<String, Object>();
                    Map<String, Object> mappingConfMap = new HashMap<String, Object>();
                    personalConfMap = gson.fromJson(personalConf, personalConfMap.getClass());
                    mappingConfMap = gson.fromJson(mappingConf, mappingConfMap.getClass());
                    String indexName = String.valueOf(Math.abs((userId + serviceId).hashCode()));
                    String hosts = String.valueOf(personalConfMap.get(ELASTIC_HOST));

                    // 这里得改变了
                    String idConf = client.get(SEARCH_CONFIG_PATH + serviceId + "/indexPK", this);
                    String id = null;
                    if (!StringUtil.isBlank(idConf)) {
                        JsonObject idObj = gson.fromJson(idConf, JsonObject.class);
                        id = null != idObj.get("indexPK") ? idObj.get("indexPK").getAsString() : null;
                    }
                    iSearchClient = new SearchClientImpl(hosts, indexName, id);
                    searchClients.put(userPid + "_" + serviceId, iSearchClient);

                } catch (Exception e) {
                    log.error("投票结果变化时，读取出错：" + e.getMessage(), e);
                }
            } else {
                log.info("---eventType---FinalWatch--" + eventType);
            }

        }

    }

}
