package com.ai.paas.ipaas.ses.mapping.model;


/**
 * 创建mapping入参
 * 
 * @author jianhua.ma
 * @version
 */
public class SesMappingApply  {

	private static final long serialVersionUID = -7276753137909397154L;
	private String userId;
	private String serviceId;
	/**
	 * 索引名称
	 */
	private String indexName;
	/**
	 * 索引类型
	 */
	private String indexType;
	/**
	 * ses引擎索引mapping定义(数据模型) 全局为"dynamic":"strict"
	 */
	private String mapping;
	private String copyto;

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public String getIndexType() {
		return indexType;
	}

	public void setIndexType(String indexType) {
		this.indexType = indexType;
	}

	public String getMapping() {
		return mapping;
	}

	public void setMapping(String mapping) {
		this.mapping = mapping;
	}

	/**
	 * userId.
	 *
	 * @return  the userId
	 * @since   JDK 1.7
	 */
	public String getUserId() {
		return userId;
	}

	/**
	 * userId.
	 *
	 * @param   userId    the userId to set
	 * @since   JDK 1.7
	 */
	public void setUserId(String userId) {
		this.userId = userId;
	}

	/**
	 * serviceId.
	 *
	 * @return  the serviceId
	 * @since   JDK 1.7
	 */
	public String getServiceId() {
		return serviceId;
	}

	/**
	 * serviceId.
	 *
	 * @param   serviceId    the serviceId to set
	 * @since   JDK 1.7
	 */
	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}

	/**
	 * copyto.
	 *
	 * @return  the copyto
	 * @since   JDK 1.7
	 */
	public String getCopyto() {
		return copyto;
	}

	/**
	 * copyto.
	 *
	 * @param   copyto    the copyto to set
	 * @since   JDK 1.7
	 */
	public void setCopyto(String copyto) {
		this.copyto = copyto;
	}

}
