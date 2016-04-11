package com.ai.paas.ipaas.ses.dao.mapper.bo;

import java.sql.Timestamp;

public class SesUserInstance {
    private Integer id;

    private String userId;

    private String hostIp;

    private Integer sesPort;

    private Integer tcpPort;

    private Integer status;

    private Integer memUse;

    private Timestamp beginTime;

    private Timestamp endTime;

    private String serviceId;

    private String serviceName;

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

    public String getHostIp() {
        return hostIp;
    }

    public void setHostIp(String hostIp) {
        this.hostIp = hostIp == null ? null : hostIp.trim();
    }

    public Integer getSesPort() {
        return sesPort;
    }

    public void setSesPort(Integer sesPort) {
        this.sesPort = sesPort;
    }

    public Integer getTcpPort() {
        return tcpPort;
    }

    public void setTcpPort(Integer tcpPort) {
        this.tcpPort = tcpPort;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Integer getMemUse() {
        return memUse;
    }

    public void setMemUse(Integer memUse) {
        this.memUse = memUse;
    }

    public Timestamp getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(Timestamp beginTime) {
        this.beginTime = beginTime;
    }

    public Timestamp getEndTime() {
        return endTime;
    }

    public void setEndTime(Timestamp endTime) {
        this.endTime = endTime;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId == null ? null : serviceId.trim();
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName == null ? null : serviceName.trim();
    }
}