package com.ai.paas.ipaas.ses.dao.interfaces;

import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserIndexWord;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserIndexWordCriteria;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface SesUserIndexWordMapper {
    int countByExample(SesUserIndexWordCriteria example);

    int deleteByExample(SesUserIndexWordCriteria example);

    int deleteByPrimaryKey(Integer id);

    int insert(SesUserIndexWord record);

    int insertBatch(List<SesUserIndexWord> list);
    
    int insertSelective(SesUserIndexWord record);

    List<SesUserIndexWord> selectByExample(SesUserIndexWordCriteria example);

    SesUserIndexWord selectByPrimaryKey(Integer id);

    int updateByExampleSelective(@Param("record") SesUserIndexWord record, @Param("example") SesUserIndexWordCriteria example);

    int updateByExample(@Param("record") SesUserIndexWord record, @Param("example") SesUserIndexWordCriteria example);

    int updateByPrimaryKeySelective(SesUserIndexWord record);

    int updateByPrimaryKey(SesUserIndexWord record);
}