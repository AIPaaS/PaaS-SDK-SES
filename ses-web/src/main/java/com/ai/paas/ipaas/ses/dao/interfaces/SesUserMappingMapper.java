package com.ai.paas.ipaas.ses.dao.interfaces;

import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserMapping;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserMappingCriteria;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface SesUserMappingMapper {
    int countByExample(SesUserMappingCriteria example);

    int deleteByExample(SesUserMappingCriteria example);

    int deleteByPrimaryKey(Integer id);

    int insert(SesUserMapping record);

    int insertSelective(SesUserMapping record);

    List<SesUserMapping> selectByExample(SesUserMappingCriteria example);

    SesUserMapping selectByPrimaryKey(Integer id);

    int updateByExampleSelective(@Param("record") SesUserMapping record, @Param("example") SesUserMappingCriteria example);

    int updateByExample(@Param("record") SesUserMapping record, @Param("example") SesUserMappingCriteria example);

    int updateByPrimaryKeySelective(SesUserMapping record);

    int updateByPrimaryKey(SesUserMapping record);
}