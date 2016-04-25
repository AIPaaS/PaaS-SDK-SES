package com.ai.paas.ipaas.ses.dao.mapper.bo;

public class SesDataimportUser {
    private Integer id;

    private String userId;

    private String userCode;

    private String sesSid;

    private Integer status;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId == null ? null : userId.trim();
    }

    public String getUserCode() {
        return userCode;
    }

    public void setUserCode(String userCode) {
        this.userCode = userCode == null ? null : userCode.trim();
    }

    public String getSesSid() {
        return sesSid;
    }

    public void setSesSid(String sesSid) {
        this.sesSid = sesSid == null ? null : sesSid.trim();
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}