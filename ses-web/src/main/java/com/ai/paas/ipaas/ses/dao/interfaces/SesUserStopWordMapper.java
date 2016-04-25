package com.ai.paas.ipaas.ses.dao.interfaces;

import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserIndexWord;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserStopWord;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesUserStopWordCriteria;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface SesUserStopWordMapper {
    int countByExample(SesUserStopWordCriteria example);

    int deleteByExample(SesUserStopWordCriteria example);

    int deleteByPrimaryKey(Integer id);

    int insert(SesUserStopWord record);

    int insertBatch(List<SesUserStopWord> list);
    
    int insertSelective(SesUserStopWord record);

    List<SesUserStopWord> selectByExample(SesUserStopWordCriteria example);

    SesUserStopWord selectByPrimaryKey(Integer id);

    int updateByExampleSelective(@Param("record") SesUserStopWord record, @Param("example") SesUserStopWordCriteria example);

    int updateByExample(@Param("record") SesUserStopWord record, @Param("example") SesUserStopWordCriteria example);

    int updateByPrimaryKeySelective(SesUserStopWord record);

    int updateByPrimaryKey(SesUserStopWord record);
}