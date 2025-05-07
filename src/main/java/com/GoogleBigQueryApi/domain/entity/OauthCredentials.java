package com.GoogleBigQueryApi.domain.entity;

public class OauthCredentials {

	String clientId;
	String clientSecret;
	String refreshToken;
	String projectId;
	String datasetId;
	final String grantType = "refresh_token";

	public OauthCredentials(String clientId, String clientSecret, String refreshToken, String projectId,
			String datasetId) {
		super();
		this.clientId = clientId;
		this.clientSecret = clientSecret;
		this.refreshToken = refreshToken;
		this.projectId = projectId;
		this.datasetId = datasetId;
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getClientSecret() {
		return clientSecret;
	}

	public void setClientSecret(String clientSecret) {
		this.clientSecret = clientSecret;
	}

	public String getRefreshToken() {
		return refreshToken;
	}

	public void setRefreshToken(String refreshToken) {
		this.refreshToken = refreshToken;
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public String getDatasetId() {
		return datasetId;
	}

	public void setDatasetId(String datasetId) {
		this.datasetId = datasetId;
	}

	public String getGrantType() {
		return grantType;
	}
}
