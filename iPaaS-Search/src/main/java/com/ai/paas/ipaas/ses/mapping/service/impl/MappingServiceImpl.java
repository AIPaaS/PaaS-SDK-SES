package com.ai.paas.ipaas.ses.mapping.service.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.ai.paas.ipaas.PaasException;
import com.ai.paas.ipaas.ServiceUtil;
import com.ai.paas.ipaas.ses.common.constants.SesConstants;
import com.ai.paas.ipaas.ses.dao.interfaces.SesUserMappingMapper;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserMapping;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserMappingCriteria;
import com.ai.paas.ipaas.ses.mapping.service.IMappingService;
@Service
@Transactional(rollbackFor = Exception.class)
public class MappingServiceImpl implements IMappingService {
	private static final transient Logger LOGGER = LoggerFactory.getLogger(MappingServiceImpl.class);

	@Override
	public SesUserMapping loadMapping(String userId, String serviceId) throws PaasException {
		List<SesUserMapping> resultList;
		try {
			SesUserMappingMapper mappingMapper = ServiceUtil.getMapper(SesUserMappingMapper.class);
			SesUserMappingCriteria example = new SesUserMappingCriteria();
			example.createCriteria().andUserIdEqualTo(userId).andServiceIdEqualTo(serviceId);
			resultList = mappingMapper.selectByExample(example);
			if(resultList != null && resultList.size() == 1){
				return resultList.get(0);
			}else{
				throw new PaasException(SesConstants.ExecResult.FAIL, SesConstants.EXPECT_ONE_RECORD_FAIL);
			}
		} catch (Exception e) {
			LOGGER.error(SesConstants.ExecResult.FAIL, e);
			throw new PaasException(SesConstants.ExecResult.FAIL, e);
			
		}
	}

	@Override
	public void editMapping(SesUserMapping mapping) throws PaasException {
		try {
			SesUserMappingMapper mappingMapper = ServiceUtil.getMapper(SesUserMappingMapper.class);
			mappingMapper.updateByPrimaryKeySelective(mapping);
		} catch (Exception e) {
			throw new PaasException(SesConstants.ExecResult.FAIL, e);
		}
	}

	@Override
	public void insertMapping(SesUserMapping mapping) throws PaasException {
		try {
        	SesUserMappingMapper mapper = ServiceUtil.getMapper(SesUserMappingMapper.class);
        	SesUserMappingCriteria sesCriteria = new SesUserMappingCriteria();
        	sesCriteria.createCriteria().andUserIdEqualTo(mapping.getUserId()).andServiceIdEqualTo(mapping.getServiceId());
			mapper.deleteByExample(sesCriteria);
			mapper.insert(mapping);
        } catch (Exception e) {
        	LOGGER.error(SesConstants.ExecResult.FAIL, e);
        	throw new PaasException(SesConstants.ExecResult.FAIL, e);
        }
		
	}
	
	
	

}
