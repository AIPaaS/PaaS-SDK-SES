package com.ai.paas.ipaas.search.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.PaasException;
import com.ai.paas.ipaas.ccs.inner.CCSComponentFactory;
import com.ai.paas.ipaas.ccs.inner.ICCSComponent;
import com.ai.paas.ipaas.ccs.zookeeper.ConfigWatcher;
import com.ai.paas.ipaas.ccs.zookeeper.ConfigWatcher.Event.EventType;
import com.ai.paas.ipaas.ccs.zookeeper.ConfigWatcherEvent;
import com.ai.paas.ipaas.search.service.impl.SearchClientImpl;
import com.ai.paas.ipaas.uac.service.UserClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;
import com.ai.paas.ipaas.uac.vo.AuthResult;
import com.ai.paas.ipaas.util.Assert;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class SearchClientFactory {
	// private static ISearchClient iSearchClient;
	private static transient final Logger log = LoggerFactory
			.getLogger(SearchClientFactory.class);
	private static Map<String, ISearchClient> searchClients = new ConcurrentHashMap<String, ISearchClient>();
	private final static String SEARCH_CONFIG_PATH = "/SES/";
	public static final String SES_MAPPING_ZK_PATH = "/SES/MAPPING/";
	private final static String ELASTIC_HOST = "hosts";
	@SuppressWarnings("unused")
	private final static String ELASTIC_MAPPING = "mapping";

	private SearchClientFactory() {

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static ISearchClient getSearchClient(AuthDescriptor ad)
			throws Exception {
		ISearchClient iSearchClient = null;
		log.info("Check Formal Parameter AuthDescriptor ...");
		Assert.notNull(ad, "AuthDescriptor对象为空");
		Assert.notNull(ad.getServiceId(), "service_id为空");
		String srvId = ad.getServiceId();
		String userPid = ad.getPid();
		if (searchClients.containsKey(userPid + "_" + srvId)) {
			iSearchClient = searchClients.get(userPid + "_" + srvId);
			return iSearchClient;
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
		ICCSComponent client = CCSComponentFactory.getConfigClient(
				authResult.getConfigAddr(), authResult.getConfigUser(),
				authResult.getConfigPasswd());

		SesWatch watch = new SesWatch(client, userPid, userId, srvId);

		String personalConf = CCSComponentFactory.getConfigClient(
				authResult.getConfigAddr(), authResult.getConfigUser(),
				authResult.getConfigPasswd()).get(SEARCH_CONFIG_PATH + srvId);

		String mappingConf = CCSComponentFactory.getConfigClient(
				authResult.getConfigAddr(), authResult.getConfigUser(),
				authResult.getConfigPasswd()).get(SES_MAPPING_ZK_PATH + srvId,
				watch);

		Gson gson = new Gson();
		Map<String, Object> personalConfMap = new HashMap<String, Object>();
		Map<String, Object> mappingConfMap = new HashMap<String, Object>();
		personalConfMap = gson.fromJson(personalConf,
				personalConfMap.getClass());

		mappingConfMap = gson.fromJson(mappingConf, mappingConfMap.getClass());
		String indexName = String
				.valueOf(Math.abs((authResult.getUserId() + srvId).hashCode()));
		String hosts = String.valueOf(personalConfMap.get(ELASTIC_HOST));

		Map map = gson.fromJson(mappingConf, Map.class);
		JsonObject properties = gson
				.fromJson((String) map.get("mapping"), JsonObject.class)
				.get(indexName).getAsJsonObject().get("properties")
				.getAsJsonObject();

		JsonObject idObj = gson
				.fromJson((String) map.get("mapping"), JsonObject.class)
				.get(indexName).getAsJsonObject().get("_id").getAsJsonObject();

		iSearchClient = new SearchClientImpl(hosts, indexName, properties,
				idObj.get("path").toString().replaceAll("\"", ""));
		searchClients.put(userPid + "_" + srvId, iSearchClient);
		return iSearchClient;
	}

	private static class SesWatch extends ConfigWatcher {
		private ICCSComponent client;
		private String userPid;
		private String userId;
		private String serviceId;

		public SesWatch(ICCSComponent client, String userPid, String userId,
				String serviceId) {
			this.client = client;
			this.serviceId = serviceId;
			this.userPid = userPid;
			this.userId = userId;
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
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

					String personalConf = client.get(SEARCH_CONFIG_PATH
							+ serviceId, this);
					String mappingConf = client.get(SES_MAPPING_ZK_PATH
							+ serviceId, this);

					Gson gson = new Gson();
					Map<String, Object> personalConfMap = new HashMap<String, Object>();
					Map<String, Object> mappingConfMap = new HashMap<String, Object>();
					personalConfMap = gson.fromJson(personalConf,
							personalConfMap.getClass());
					mappingConfMap = gson.fromJson(mappingConf,
							mappingConfMap.getClass());
					String indexName = String.valueOf(Math
							.abs((userId + serviceId).hashCode()));
					String hosts = String.valueOf(personalConfMap
							.get(ELASTIC_HOST));

					Map map = gson.fromJson(mappingConf, Map.class);
					JsonObject properties = gson
							.fromJson((String) map.get("mapping"),
									JsonObject.class).get(indexName)
							.getAsJsonObject().get("properties")
							.getAsJsonObject();
					JsonObject idObj = gson
							.fromJson((String) map.get("mapping"),
									JsonObject.class).get(indexName)
							.getAsJsonObject().get("_id").getAsJsonObject();
					iSearchClient = new SearchClientImpl(hosts, indexName,
							properties, idObj.get("path").toString()
									.replaceAll("\"", ""));
					searchClients.put(userPid + "_" + serviceId, iSearchClient);

				} catch (PaasException e) {
					log.error("投票结果变化时，读取出错：" + e.getMessage(), e);
				}
			} else {
				log.info("---eventType---FinalWatch--" + eventType);
			}

		}

	}

}
