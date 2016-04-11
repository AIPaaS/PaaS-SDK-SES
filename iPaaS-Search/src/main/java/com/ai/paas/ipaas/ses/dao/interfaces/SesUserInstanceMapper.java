package com.ai.paas.ipaas.ses.dao.interfaces;

import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserInstance;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserInstanceCriteria;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface SesUserInstanceMapper {
    int countByExample(SesUserInstanceCriteria example);

    int deleteByExample(SesUserInstanceCriteria example);

    int deleteByPrimaryKey(Integer id);

    int insert(SesUserInstance record);

    int insertSelective(SesUserInstance record);

    List<SesUserInstance> selectByExample(SesUserInstanceCriteria example);

    SesUserInstance selectByPrimaryKey(Integer id);

    int updateByExampleSelective(@Param("record") SesUserInstance record, @Param("example") SesUserInstanceCriteria example);

    int updateByExample(@Param("record") SesUserInstance record, @Param("example") SesUserInstanceCriteria example);

    int updateByPrimaryKeySelective(SesUserInstance record);

    int updateByPrimaryKey(SesUserInstance record);
}