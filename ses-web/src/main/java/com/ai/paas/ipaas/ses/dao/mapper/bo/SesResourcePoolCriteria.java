package com.ai.paas.ipaas.ses.dao.mapper.bo;

import java.util.ArrayList;
import java.util.List;

public class SesResourcePoolCriteria {
    protected String orderByClause;

    protected boolean distinct;

    protected List<Criteria> oredCriteria;

    protected Integer limitStart;

    protected Integer limitEnd;

    public SesResourcePoolCriteria() {
        oredCriteria = new ArrayList<Criteria>();
    }

    public void setOrderByClause(String orderByClause) {
        this.orderByClause = orderByClause;
    }

    public String getOrderByClause() {
        return orderByClause;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

    public boolean isDistinct() {
        return distinct;
    }

    public List<Criteria> getOredCriteria() {
        return oredCriteria;
    }

    public void or(Criteria criteria) {
        oredCriteria.add(criteria);
    }

    public Criteria or() {
        Criteria criteria = createCriteriaInternal();
        oredCriteria.add(criteria);
        return criteria;
    }

    public Criteria createCriteria() {
        Criteria criteria = createCriteriaInternal();
        if (oredCriteria.size() == 0) {
            oredCriteria.add(criteria);
        }
        return criteria;
    }

    protected Criteria createCriteriaInternal() {
        Criteria criteria = new Criteria();
        return criteria;
    }

    public void clear() {
        oredCriteria.clear();
        orderByClause = null;
        distinct = false;
    }

    public void setLimitStart(Integer limitStart) {
        this.limitStart=limitStart;
    }

    public Integer getLimitStart() {
        return limitStart;
    }

    public void setLimitEnd(Integer limitEnd) {
        this.limitEnd=limitEnd;
    }

    public Integer getLimitEnd() {
        return limitEnd;
    }

    protected abstract static class GeneratedCriteria {
        protected List<Criterion> criteria;

        protected GeneratedCriteria() {
            super();
            criteria = new ArrayList<Criterion>();
        }

        public boolean isValid() {
            return criteria.size() > 0;
        }

        public List<Criterion> getAllCriteria() {
            return criteria;
        }

        public List<Criterion> getCriteria() {
            return criteria;
        }

        protected void addCriterion(String condition) {
            if (condition == null) {
                throw new RuntimeException("Value for condition cannot be null");
            }
            criteria.add(new Criterion(condition));
        }

        protected void addCriterion(String condition, Object value, String property) {
            if (value == null) {
                throw new RuntimeException("Value for " + property + " cannot be null");
            }
            criteria.add(new Criterion(condition, value));
        }

        protected void addCriterion(String condition, Object value1, Object value2, String property) {
            if (value1 == null || value2 == null) {
                throw new RuntimeException("Between values for " + property + " cannot be null");
            }
            criteria.add(new Criterion(condition, value1, value2));
        }

        public Criteria andIdIsNull() {
            addCriterion("id is null");
            return (Criteria) this;
        }

        public Criteria andIdIsNotNull() {
            addCriterion("id is not null");
            return (Criteria) this;
        }

        public Criteria andIdEqualTo(Integer value) {
            addCriterion("id =", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdNotEqualTo(Integer value) {
            addCriterion("id <>", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdGreaterThan(Integer value) {
            addCriterion("id >", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdGreaterThanOrEqualTo(Integer value) {
            addCriterion("id >=", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdLessThan(Integer value) {
            addCriterion("id <", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdLessThanOrEqualTo(Integer value) {
            addCriterion("id <=", value, "id");
            return (Criteria) this;
        }

        public Criteria andIdIn(List<Integer> values) {
            addCriterion("id in", values, "id");
            return (Criteria) this;
        }

        public Criteria andIdNotIn(List<Integer> values) {
            addCriterion("id not in", values, "id");
            return (Criteria) this;
        }

        public Criteria andIdBetween(Integer value1, Integer value2) {
            addCriterion("id between", value1, value2, "id");
            return (Criteria) this;
        }

        public Criteria andIdNotBetween(Integer value1, Integer value2) {
            addCriterion("id not between", value1, value2, "id");
            return (Criteria) this;
        }

        public Criteria andHostIpIsNull() {
            addCriterion("host_ip is null");
            return (Criteria) this;
        }

        public Criteria andHostIpIsNotNull() {
            addCriterion("host_ip is not null");
            return (Criteria) this;
        }

        public Criteria andHostIpEqualTo(String value) {
            addCriterion("host_ip =", value, "hostIp");
            return (Criteria) this;
        }

        public Criteria andHostIpNotEqualTo(String value) {
            addCriterion("host_ip <>", value, "hostIp");
            return (Criteria) this;
        }

        public Criteria andHostIpGreaterThan(String value) {
            addCriterion("host_ip >", value, "hostIp");
            return (Criteria) this;
        }

        public Criteria andHostIpGreaterThanOrEqualTo(String value) {
            addCriterion("host_ip >=", value, "hostIp");
            return (Criteria) this;
        }

        public Criteria andHostIpLessThan(String value) {
            addCriterion("host_ip <", value, "hostIp");
            return (Criteria) this;
        }

        public Criteria andHostIpLessThanOrEqualTo(String value) {
            addCriterion("host_ip <=", value, "hostIp");
            return (Criteria) this;
        }

        public Criteria andHostIpLike(String value) {
            addCriterion("host_ip like", value, "hostIp");
            return (Criteria) this;
        }

        public Criteria andHostIpNotLike(String value) {
            addCriterion("host_ip not like", value, "hostIp");
            return (Criteria) this;
        }

        public Criteria andHostIpIn(List<String> values) {
            addCriterion("host_ip in", values, "hostIp");
            return (Criteria) this;
        }

        public Criteria andHostIpNotIn(List<String> values) {
            addCriterion("host_ip not in", values, "hostIp");
            return (Criteria) this;
        }

        public Criteria andHostIpBetween(String value1, String value2) {
            addCriterion("host_ip between", value1, value2, "hostIp");
            return (Criteria) this;
        }

        public Criteria andHostIpNotBetween(String value1, String value2) {
            addCriterion("host_ip not between", value1, value2, "hostIp");
            return (Criteria) this;
        }

        public Criteria andStatusIsNull() {
            addCriterion("status is null");
            return (Criteria) this;
        }

        public Criteria andStatusIsNotNull() {
            addCriterion("status is not null");
            return (Criteria) this;
        }

        public Criteria andStatusEqualTo(Integer value) {
            addCriterion("status =", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusNotEqualTo(Integer value) {
            addCriterion("status <>", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusGreaterThan(Integer value) {
            addCriterion("status >", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusGreaterThanOrEqualTo(Integer value) {
            addCriterion("status >=", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusLessThan(Integer value) {
            addCriterion("status <", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusLessThanOrEqualTo(Integer value) {
            addCriterion("status <=", value, "status");
            return (Criteria) this;
        }

        public Criteria andStatusIn(List<Integer> values) {
            addCriterion("status in", values, "status");
            return (Criteria) this;
        }

        public Criteria andStatusNotIn(List<Integer> values) {
            addCriterion("status not in", values, "status");
            return (Criteria) this;
        }

        public Criteria andStatusBetween(Integer value1, Integer value2) {
            addCriterion("status between", value1, value2, "status");
            return (Criteria) this;
        }

        public Criteria andStatusNotBetween(Integer value1, Integer value2) {
            addCriterion("status not between", value1, value2, "status");
            return (Criteria) this;
        }

        public Criteria andPortMinIsNull() {
            addCriterion("port_min is null");
            return (Criteria) this;
        }

        public Criteria andPortMinIsNotNull() {
            addCriterion("port_min is not null");
            return (Criteria) this;
        }

        public Criteria andPortMinEqualTo(Integer value) {
            addCriterion("port_min =", value, "portMin");
            return (Criteria) this;
        }

        public Criteria andPortMinNotEqualTo(Integer value) {
            addCriterion("port_min <>", value, "portMin");
            return (Criteria) this;
        }

        public Criteria andPortMinGreaterThan(Integer value) {
            addCriterion("port_min >", value, "portMin");
            return (Criteria) this;
        }

        public Criteria andPortMinGreaterThanOrEqualTo(Integer value) {
            addCriterion("port_min >=", value, "portMin");
            return (Criteria) this;
        }

        public Criteria andPortMinLessThan(Integer value) {
            addCriterion("port_min <", value, "portMin");
            return (Criteria) this;
        }

        public Criteria andPortMinLessThanOrEqualTo(Integer value) {
            addCriterion("port_min <=", value, "portMin");
            return (Criteria) this;
        }

        public Criteria andPortMinIn(List<Integer> values) {
            addCriterion("port_min in", values, "portMin");
            return (Criteria) this;
        }

        public Criteria andPortMinNotIn(List<Integer> values) {
            addCriterion("port_min not in", values, "portMin");
            return (Criteria) this;
        }

        public Criteria andPortMinBetween(Integer value1, Integer value2) {
            addCriterion("port_min between", value1, value2, "portMin");
            return (Criteria) this;
        }

        public Criteria andPortMinNotBetween(Integer value1, Integer value2) {
            addCriterion("port_min not between", value1, value2, "portMin");
            return (Criteria) this;
        }

        public Criteria andPortMaxIsNull() {
            addCriterion("port_max is null");
            return (Criteria) this;
        }

        public Criteria andPortMaxIsNotNull() {
            addCriterion("port_max is not null");
            return (Criteria) this;
        }

        public Criteria andPortMaxEqualTo(Integer value) {
            addCriterion("port_max =", value, "portMax");
            return (Criteria) this;
        }

        public Criteria andPortMaxNotEqualTo(Integer value) {
            addCriterion("port_max <>", value, "portMax");
            return (Criteria) this;
        }

        public Criteria andPortMaxGreaterThan(Integer value) {
            addCriterion("port_max >", value, "portMax");
            return (Criteria) this;
        }

        public Criteria andPortMaxGreaterThanOrEqualTo(Integer value) {
            addCriterion("port_max >=", value, "portMax");
            return (Criteria) this;
        }

        public Criteria andPortMaxLessThan(Integer value) {
            addCriterion("port_max <", value, "portMax");
            return (Criteria) this;
        }

        public Criteria andPortMaxLessThanOrEqualTo(Integer value) {
            addCriterion("port_max <=", value, "portMax");
            return (Criteria) this;
        }

        public Criteria andPortMaxIn(List<Integer> values) {
            addCriterion("port_max in", values, "portMax");
            return (Criteria) this;
        }

        public Criteria andPortMaxNotIn(List<Integer> values) {
            addCriterion("port_max not in", values, "portMax");
            return (Criteria) this;
        }

        public Criteria andPortMaxBetween(Integer value1, Integer value2) {
            addCriterion("port_max between", value1, value2, "portMax");
            return (Criteria) this;
        }

        public Criteria andPortMaxNotBetween(Integer value1, Integer value2) {
            addCriterion("port_max not between", value1, value2, "portMax");
            return (Criteria) this;
        }

        public Criteria andMemTotalIsNull() {
            addCriterion("mem_total is null");
            return (Criteria) this;
        }

        public Criteria andMemTotalIsNotNull() {
            addCriterion("mem_total is not null");
            return (Criteria) this;
        }

        public Criteria andMemTotalEqualTo(Integer value) {
            addCriterion("mem_total =", value, "memTotal");
            return (Criteria) this;
        }

        public Criteria andMemTotalNotEqualTo(Integer value) {
            addCriterion("mem_total <>", value, "memTotal");
            return (Criteria) this;
        }

        public Criteria andMemTotalGreaterThan(Integer value) {
            addCriterion("mem_total >", value, "memTotal");
            return (Criteria) this;
        }

        public Criteria andMemTotalGreaterThanOrEqualTo(Integer value) {
            addCriterion("mem_total >=", value, "memTotal");
            return (Criteria) this;
        }

        public Criteria andMemTotalLessThan(Integer value) {
            addCriterion("mem_total <", value, "memTotal");
            return (Criteria) this;
        }

        public Criteria andMemTotalLessThanOrEqualTo(Integer value) {
            addCriterion("mem_total <=", value, "memTotal");
            return (Criteria) this;
        }

        public Criteria andMemTotalIn(List<Integer> values) {
            addCriterion("mem_total in", values, "memTotal");
            return (Criteria) this;
        }

        public Criteria andMemTotalNotIn(List<Integer> values) {
            addCriterion("mem_total not in", values, "memTotal");
            return (Criteria) this;
        }

        public Criteria andMemTotalBetween(Integer value1, Integer value2) {
            addCriterion("mem_total between", value1, value2, "memTotal");
            return (Criteria) this;
        }

        public Criteria andMemTotalNotBetween(Integer value1, Integer value2) {
            addCriterion("mem_total not between", value1, value2, "memTotal");
            return (Criteria) this;
        }

        public Criteria andMemUsedIsNull() {
            addCriterion("mem_used is null");
            return (Criteria) this;
        }

        public Criteria andMemUsedIsNotNull() {
            addCriterion("mem_used is not null");
            return (Criteria) this;
        }

        public Criteria andMemUsedEqualTo(Integer value) {
            addCriterion("mem_used =", value, "memUsed");
            return (Criteria) this;
        }

        public Criteria andMemUsedNotEqualTo(Integer value) {
            addCriterion("mem_used <>", value, "memUsed");
            return (Criteria) this;
        }

        public Criteria andMemUsedGreaterThan(Integer value) {
            addCriterion("mem_used >", value, "memUsed");
            return (Criteria) this;
        }

        public Criteria andMemUsedGreaterThanOrEqualTo(Integer value) {
            addCriterion("mem_used >=", value, "memUsed");
            return (Criteria) this;
        }

        public Criteria andMemUsedLessThan(Integer value) {
            addCriterion("mem_used <", value, "memUsed");
            return (Criteria) this;
        }

        public Criteria andMemUsedLessThanOrEqualTo(Integer value) {
            addCriterion("mem_used <=", value, "memUsed");
            return (Criteria) this;
        }

        public Criteria andMemUsedIn(List<Integer> values) {
            addCriterion("mem_used in", values, "memUsed");
            return (Criteria) this;
        }

        public Criteria andMemUsedNotIn(List<Integer> values) {
            addCriterion("mem_used not in", values, "memUsed");
            return (Criteria) this;
        }

        public Criteria andMemUsedBetween(Integer value1, Integer value2) {
            addCriterion("mem_used between", value1, value2, "memUsed");
            return (Criteria) this;
        }

        public Criteria andMemUsedNotBetween(Integer value1, Integer value2) {
            addCriterion("mem_used not between", value1, value2, "memUsed");
            return (Criteria) this;
        }

        public Criteria andAgentPortIsNull() {
            addCriterion("agent_port is null");
            return (Criteria) this;
        }

        public Criteria andAgentPortIsNotNull() {
            addCriterion("agent_port is not null");
            return (Criteria) this;
        }

        public Criteria andAgentPortEqualTo(Integer value) {
            addCriterion("agent_port =", value, "agentPort");
            return (Criteria) this;
        }

        public Criteria andAgentPortNotEqualTo(Integer value) {
            addCriterion("agent_port <>", value, "agentPort");
            return (Criteria) this;
        }

        public Criteria andAgentPortGreaterThan(Integer value) {
            addCriterion("agent_port >", value, "agentPort");
            return (Criteria) this;
        }

        public Criteria andAgentPortGreaterThanOrEqualTo(Integer value) {
            addCriterion("agent_port >=", value, "agentPort");
            return (Criteria) this;
        }

        public Criteria andAgentPortLessThan(Integer value) {
            addCriterion("agent_port <", value, "agentPort");
            return (Criteria) this;
        }

        public Criteria andAgentPortLessThanOrEqualTo(Integer value) {
            addCriterion("agent_port <=", value, "agentPort");
            return (Criteria) this;
        }

        public Criteria andAgentPortIn(List<Integer> values) {
            addCriterion("agent_port in", values, "agentPort");
            return (Criteria) this;
        }

        public Criteria andAgentPortNotIn(List<Integer> values) {
            addCriterion("agent_port not in", values, "agentPort");
            return (Criteria) this;
        }

        public Criteria andAgentPortBetween(Integer value1, Integer value2) {
            addCriterion("agent_port between", value1, value2, "agentPort");
            return (Criteria) this;
        }

        public Criteria andAgentPortNotBetween(Integer value1, Integer value2) {
            addCriterion("agent_port not between", value1, value2, "agentPort");
            return (Criteria) this;
        }

        public Criteria andBinPathIsNull() {
            addCriterion("bin_path is null");
            return (Criteria) this;
        }

        public Criteria andBinPathIsNotNull() {
            addCriterion("bin_path is not null");
            return (Criteria) this;
        }

        public Criteria andBinPathEqualTo(String value) {
            addCriterion("bin_path =", value, "binPath");
            return (Criteria) this;
        }

        public Criteria andBinPathNotEqualTo(String value) {
            addCriterion("bin_path <>", value, "binPath");
            return (Criteria) this;
        }

        public Criteria andBinPathGreaterThan(String value) {
            addCriterion("bin_path >", value, "binPath");
            return (Criteria) this;
        }

        public Criteria andBinPathGreaterThanOrEqualTo(String value) {
            addCriterion("bin_path >=", value, "binPath");
            return (Criteria) this;
        }

        public Criteria andBinPathLessThan(String value) {
            addCriterion("bin_path <", value, "binPath");
            return (Criteria) this;
        }

        public Criteria andBinPathLessThanOrEqualTo(String value) {
            addCriterion("bin_path <=", value, "binPath");
            return (Criteria) this;
        }

        public Criteria andBinPathLike(String value) {
            addCriterion("bin_path like", value, "binPath");
            return (Criteria) this;
        }

        public Criteria andBinPathNotLike(String value) {
            addCriterion("bin_path not like", value, "binPath");
            return (Criteria) this;
        }

        public Criteria andBinPathIn(List<String> values) {
            addCriterion("bin_path in", values, "binPath");
            return (Criteria) this;
        }

        public Criteria andBinPathNotIn(List<String> values) {
            addCriterion("bin_path not in", values, "binPath");
            return (Criteria) this;
        }

        public Criteria andBinPathBetween(String value1, String value2) {
            addCriterion("bin_path between", value1, value2, "binPath");
            return (Criteria) this;
        }

        public Criteria andBinPathNotBetween(String value1, String value2) {
            addCriterion("bin_path not between", value1, value2, "binPath");
            return (Criteria) this;
        }

        public Criteria andUserPathIsNull() {
            addCriterion("user_path is null");
            return (Criteria) this;
        }

        public Criteria andUserPathIsNotNull() {
            addCriterion("user_path is not null");
            return (Criteria) this;
        }

        public Criteria andUserPathEqualTo(String value) {
            addCriterion("user_path =", value, "userPath");
            return (Criteria) this;
        }

        public Criteria andUserPathNotEqualTo(String value) {
            addCriterion("user_path <>", value, "userPath");
            return (Criteria) this;
        }

        public Criteria andUserPathGreaterThan(String value) {
            addCriterion("user_path >", value, "userPath");
            return (Criteria) this;
        }

        public Criteria andUserPathGreaterThanOrEqualTo(String value) {
            addCriterion("user_path >=", value, "userPath");
            return (Criteria) this;
        }

        public Criteria andUserPathLessThan(String value) {
            addCriterion("user_path <", value, "userPath");
            return (Criteria) this;
        }

        public Criteria andUserPathLessThanOrEqualTo(String value) {
            addCriterion("user_path <=", value, "userPath");
            return (Criteria) this;
        }

        public Criteria andUserPathLike(String value) {
            addCriterion("user_path like", value, "userPath");
            return (Criteria) this;
        }

        public Criteria andUserPathNotLike(String value) {
            addCriterion("user_path not like", value, "userPath");
            return (Criteria) this;
        }

        public Criteria andUserPathIn(List<String> values) {
            addCriterion("user_path in", values, "userPath");
            return (Criteria) this;
        }

        public Criteria andUserPathNotIn(List<String> values) {
            addCriterion("user_path not in", values, "userPath");
            return (Criteria) this;
        }

        public Criteria andUserPathBetween(String value1, String value2) {
            addCriterion("user_path between", value1, value2, "userPath");
            return (Criteria) this;
        }

        public Criteria andUserPathNotBetween(String value1, String value2) {
            addCriterion("user_path not between", value1, value2, "userPath");
            return (Criteria) this;
        }
    }

    public static class Criteria extends GeneratedCriteria {

        protected Criteria() {
            super();
        }
    }

    public static class Criterion {
        private String condition;

        private Object value;

        private Object secondValue;

        private boolean noValue;

        private boolean singleValue;

        private boolean betweenValue;

        private boolean listValue;

        private String typeHandler;

        public String getCondition() {
            return condition;
        }

        public Object getValue() {
            return value;
        }

        public Object getSecondValue() {
            return secondValue;
        }

        public boolean isNoValue() {
            return noValue;
        }

        public boolean isSingleValue() {
            return singleValue;
        }

        public boolean isBetweenValue() {
            return betweenValue;
        }

        public boolean isListValue() {
            return listValue;
        }

        public String getTypeHandler() {
            return typeHandler;
        }

        protected Criterion(String condition) {
            super();
            this.condition = condition;
            this.typeHandler = null;
            this.noValue = true;
        }

        protected Criterion(String condition, Object value, String typeHandler) {
            super();
            this.condition = condition;
            this.value = value;
            this.typeHandler = typeHandler;
            if (value instanceof List<?>) {
                this.listValue = true;
            } else {
                this.singleValue = true;
            }
        }

        protected Criterion(String condition, Object value) {
            this(condition, value, null);
        }

        protected Criterion(String condition, Object value, Object secondValue, String typeHandler) {
            super();
            this.condition = condition;
            this.value = value;
            this.secondValue = secondValue;
            this.typeHandler = typeHandler;
            this.betweenValue = true;
        }

        protected Criterion(String condition, Object value, Object secondValue) {
            this(condition, value, secondValue, null);
        }
    }
}