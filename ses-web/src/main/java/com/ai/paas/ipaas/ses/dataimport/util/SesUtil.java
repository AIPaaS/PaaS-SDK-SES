package com.ai.paas.ipaas.ses.dataimport.util;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.paas.ipaas.search.service.ISearchClient;
import com.ai.paas.ipaas.search.service.SearchClientFactory;
import com.ai.paas.ipaas.uac.vo.AuthDescriptor;

public class SesUtil {
	private static final transient Logger log = LoggerFactory.getLogger(SesUtil.class);

	
	private static Map<String,ISearchClient> iSearchClients = new HashMap<String,ISearchClient>();

	private SesUtil(){
		
	}
	public static void initSesClient(String userName,String serviceName,String password,String addrAUth) throws Exception{
		if(!iSearchClients.containsKey(userName+"_"+serviceName)){
			ISearchClient isc = getSESInstance(userName,serviceName,password,addrAUth);
			if(isc!=null)
				iSearchClients.put(userName+"_"+serviceName, isc);
		}
		
	}
	
	
	public static ISearchClient getSesClient(String userName,String serviceId){
		return iSearchClients.get(userName+"_"+serviceId);
	}
	public static ISearchClient getSesClient(String key){
		return iSearchClients.get(key);
	}

	
	public static ISearchClient getSESInstance(String user, String serviceId,
			String servicePwd,String addrAUth) throws Exception {
		AuthDescriptor ad = new AuthDescriptor(addrAUth, user, servicePwd,
				serviceId);
		log.info("auth {}, userName {},pwd {}, sid {}",addrAUth,user,servicePwd,serviceId);
		ISearchClient iSearchClient = null;
		try {
			iSearchClient = SearchClientFactory.getSearchClient(ad);
		} catch (Exception e) {
			log.error(e.getMessage(),e);
			throw e;
		}
		return iSearchClient;
	}
	
	
	
}
