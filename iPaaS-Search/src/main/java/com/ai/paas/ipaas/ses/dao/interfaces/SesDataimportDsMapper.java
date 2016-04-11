package com.ai.paas.ipaas.ses.dao.interfaces;

import com.ai.paas.ipaas.ses.dao.mapper.bo.SesDataimportDs;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesDataimportDsCriteria;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface SesDataimportDsMapper {
    int countByExample(SesDataimportDsCriteria example);

    int deleteByExample(SesDataimportDsCriteria example);

    int deleteByPrimaryKey(Integer id);

    int insert(SesDataimportDs record);

    int insertSelective(SesDataimportDs record);

    List<SesDataimportDs> selectByExample(SesDataimportDsCriteria example);

    SesDataimportDs selectByPrimaryKey(Integer id);

    int updateByExampleSelective(@Param("record") SesDataimportDs record, @Param("example") SesDataimportDsCriteria example);

    int updateByExample(@Param("record") SesDataimportDs record, @Param("example") SesDataimportDsCriteria example);

    int updateByPrimaryKeySelective(SesDataimportDs record);

    int updateByPrimaryKey(SesDataimportDs record);
}