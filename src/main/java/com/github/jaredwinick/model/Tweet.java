package com.github.jaredwinick.model;

import java.io.Serializable;
import java.util.Date;

import org.codehaus.jackson.annotate.JsonProperty;

public class Tweet implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4216050778798586221L;
	
	private String idStr;
	private Long userId;
	private String userName;
	private Date createdAt;
	
	public String getIdStr() {
		return idStr;
	}
	
	@JsonProperty("id_str")
	public void setIdStr(String idStr) {
		this.idStr = idStr;
	}
	public Long getUserId() {
		return userId;
	}
	
	@JsonProperty("user_id")
	public void setUserId(Long userId) {
		this.userId = userId;
	}
	public String getUserName() {
		return userName;
	}
	
	@JsonProperty("user_name")
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public Date getCreatedAt() {
		return createdAt;
	}
	
	@JsonProperty("created_at")
	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}
}
