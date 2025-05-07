package com.GoogleBigQueryApi.constants;

public class GoogleBigQueryURLs {
	public static final String OAUTH_ACCESS_TOKEN_URL = "https://accounts.google.com/o/oauth2/token";
	public static final String GOOGLE_DRIVE_FILES_URL = "https://bigquery.googleapis.com/bigquery/v2/projects";
	public static final String BIGQUERY_DATASETS_URL = "https://bigquery.googleapis.com/bigquery/v2/projects/%s/datasets";
	public static final String BIGQUERY_TABLE_NAME_URL = "https://bigquery.googleapis.com/bigquery/v2/projects/%s/datasets/%s/tables";
	public static final String BIGQUERY_COLUMN_NAME_URL = "https://bigquery.googleapis.com/bigquery/v2/projects/%s/datasets/%s/tables/%s";
	public static final String BIGQUERY_TABLE_DATA_URL = "https://bigquery.googleapis.com/bigquery/v2/projects/%s/datasets/%s/tables/%s/data";

	private GoogleBigQueryURLs() {
	}
}
