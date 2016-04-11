package com.ai.paas.ipaas.ses.dao.mapper.bo;

public class SesDataimportSql {
    private Integer id;

    private Integer duId;

    private Integer dsId;

    private String alias;

    private Integer isPrimary;

    private String dsAlias;

    private String info;

    private Integer status;

    private Integer groupId;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getDuId() {
        return duId;
    }

    public void setDuId(Integer duId) {
        this.duId = duId;
    }

    public Integer getDsId() {
        return dsId;
    }

    public void setDsId(Integer dsId) {
        this.dsId = dsId;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias == null ? null : alias.trim();
    }

    public Integer getIsPrimary() {
        return isPrimary;
    }

    public void setIsPrimary(Integer isPrimary) {
        this.isPrimary = isPrimary;
    }

    public String getDsAlias() {
        return dsAlias;
    }

    public void setDsAlias(String dsAlias) {
        this.dsAlias = dsAlias == null ? null : dsAlias.trim();
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info == null ? null : info.trim();
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Integer getGroupId() {
        return groupId;
    }

    public void setGroupId(Integer groupId) {
        this.groupId = groupId;
    }
}