package com.ai.paas.ipaas.ses.overview.service.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ai.paas.ipaas.PaasException;
import com.ai.paas.ipaas.ServiceUtil;
import com.ai.paas.ipaas.ses.common.constants.SesConstants;
import com.ai.paas.ipaas.ses.dao.interfaces.SesUserInstanceMapper;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserInstance;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserInstanceCriteria;
import com.ai.paas.ipaas.ses.overview.service.IOverViewService;
@Service
@Transactional(rollbackFor = Exception.class)
public class OverViewServiceImpl implements IOverViewService {
	
	private static final transient Logger LOGGER = LoggerFactory.getLogger(OverViewServiceImpl.class);

	@Override
	public SesUserInstance queryClient(String userId, String serviceId)
			throws PaasException {
		List<SesUserInstance> resultList;
		try {
			SesUserInstanceMapper mapper = ServiceUtil.getMapper(SesUserInstanceMapper.class);
			SesUserInstanceCriteria example = new SesUserInstanceCriteria();
			example.createCriteria().andUserIdEqualTo(userId).andServiceIdEqualTo(serviceId);
			resultList = mapper.selectByExample(example);
			if(resultList != null && resultList.size() > 1){
				return resultList.get(0);
			}else{
				throw new PaasException(SesConstants.ExecResult.FAIL, SesConstants.EXPECT_ONE_RECORD_FAIL);
			}
		} catch (Exception e) {
			LOGGER.error(SesConstants.ExecResult.FAIL, e);
			throw new PaasException(SesConstants.ExecResult.FAIL, e);
		}
	}
	
	
	

}
