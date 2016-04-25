package com.ai.paas.ipaas.ses.dataimport.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LineMapParam {
	//roles    rolse.menus sql别名  结果集的结构、等级
	private String alias;
	//1对多
	private boolean isMany;
	//    空       user.userid
	private String paf;

	//user.userid  roles.roleid
	private String af;
	//结果集
	private Object rs;
	
	private String cv;
	public String getAlias() {
		return alias;
	}
	public void setAlias(String alias) {
		this.alias = alias;
	}
	public boolean isMany() {
		return isMany;
	}
	public void setMany(boolean isMany) {
		this.isMany = isMany;
	}
	public String getAf() {
		return af;
	}
	public void setAf(String af) {
		this.af = af;
	}
	public Object getRs() {
		return rs;
	}
	public void setRs(Object rs) {
		this.rs = rs;
	}
	
	public String getPaf() {
		return paf;
	}
	public void setPaf(String paf) {
		this.paf = paf;
	}
	
	
	public String getCv() {
		return cv;
	}
	public void setCv(String cv) {
		this.cv = cv;
	}
	public void put(String key,Object value){
		if(!isMany&&rs instanceof HashMap){
			HashMap map = (HashMap) rs;
			map.put(key, value);
		}
	}
	public void add(Map map,String cv){
		if(isMany&&rs instanceof ArrayList){
			ArrayList lists = (ArrayList) rs;
			//cv变化时加上标识
			if(this.cv==null||"".equals(this.cv)){
				this.cv= cv;
			}else{
				if(!cv.equals(this.cv)){
					Map tempmap = new HashMap();
					tempmap.put("flagBreak", "-1");
					lists.add(tempmap);
				}
				this.cv= cv;
			}
			lists.add(map);
		}
	}
}
