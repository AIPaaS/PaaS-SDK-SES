package com.ai.paas.ipaas.ses.dao.interfaces;

import com.ai.paas.ipaas.ses.dao.mapper.bo.SesDataimportUser;
import com.ai.paas.ipaas.ses.dao.mapper.bo.SesDataimportUserCriteria;
import java.util.List;
import org.apache.ibatis.annotations.Param;

public interface SesDataimportUserMapper {
    int countByExample(SesDataimportUserCriteria example);

    int deleteByExample(SesDataimportUserCriteria example);

    int deleteByPrimaryKey(Integer id);

    int insert(SesDataimportUser record);

    int insertSelective(SesDataimportUser record);

    List<SesDataimportUser> selectByExample(SesDataimportUserCriteria example);

    SesDataimportUser selectByPrimaryKey(Integer id);

    int updateByExampleSelective(@Param("record") SesDataimportUser record, @Param("example") SesDataimportUserCriteria example);

    int updateByExample(@Param("record") SesDataimportUser record, @Param("example") SesDataimportUserCriteria example);

    int updateByPrimaryKeySelective(SesDataimportUser record);

    int updateByPrimaryKey(SesDataimportUser record);
}