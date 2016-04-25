package com.ai.paas.ipaas.ses.dao.interfaces;

import com.ai.paas.ipaas.ses.dao.mapper.bo.SesResourcePool;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesResourcePoolCriteria;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface SesResourcePoolMapper {
    int countByExample(SesResourcePoolCriteria example);

    int deleteByExample(SesResourcePoolCriteria example);

    int deleteByPrimaryKey(Integer id);

    int insert(SesResourcePool record);

    int insertSelective(SesResourcePool record);

    List<SesResourcePool> selectByExample(SesResourcePoolCriteria example);

    SesResourcePool selectByPrimaryKey(Integer id);

    int updateByExampleSelective(@Param("record") SesResourcePool record, @Param("example") SesResourcePoolCriteria example);

    int updateByExample(@Param("record") SesResourcePool record, @Param("example") SesResourcePoolCriteria example);

    int updateByPrimaryKeySelective(SesResourcePool record);

    int updateByPrimaryKey(SesResourcePool record);
}