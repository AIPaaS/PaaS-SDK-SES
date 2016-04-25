package com.ai.paas.ipaas.ses.dao.interfaces;

import com.ai.paas.ipaas.ses.dao.mapper.bo.SesDataimportSql;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesDataimportSqlCriteria;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface SesDataimportSqlMapper {
    int countByExample(SesDataimportSqlCriteria example);

    int deleteByExample(SesDataimportSqlCriteria example);

    int deleteByPrimaryKey(Integer id);

    int insert(SesDataimportSql record);

    int insertSelective(SesDataimportSql record);

    List<SesDataimportSql> selectByExample(SesDataimportSqlCriteria example);

    SesDataimportSql selectByPrimaryKey(Integer id);

    int updateByExampleSelective(@Param("record") SesDataimportSql record, @Param("example") SesDataimportSqlCriteria example);

    int updateByExample(@Param("record") SesDataimportSql record, @Param("example") SesDataimportSqlCriteria example);

    int updateByPrimaryKeySelective(SesDataimportSql record);

    int updateByPrimaryKey(SesDataimportSql record);
}