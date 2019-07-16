package com.ai.paas.ipaas.ses;

import java.util.Date;

public class User {
	private String userId = null;
	private String name = null;
	private int age = 0;
	private Date created = null;

	public User() {
		
	}
	
	public User(String userId, String name, int age, Date created) {
		this.userId = userId;
		this.name = name;
		this.age = age;
		this.created = created;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

}
