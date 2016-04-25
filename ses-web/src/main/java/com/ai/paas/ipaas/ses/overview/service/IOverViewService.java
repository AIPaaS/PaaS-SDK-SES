package com.ai.paas.ipaas.ses.overview.service;

import com.ai.paas.ipaas.PaasException;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserInstance;

public interface IOverViewService {
	/**
	 * 根据userid serviceid查询主机实例
	 * @param userId
	 * @param serviceId
	 * @return
	 * @throws PaasException 
	 */
	SesUserInstance  queryClient(String userId,String serviceId) throws PaasException;
	
}
