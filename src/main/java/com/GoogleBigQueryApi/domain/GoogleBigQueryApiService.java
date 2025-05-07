package com.GoogleBigQueryApi.domain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.springframework.stereotype.Service;

import com.GoogleBigQueryApi.constants.GoogleBigQueryURLs;
import com.GoogleBigQueryApi.domain.entity.OauthCredentials;
import com.GoogleBigQueryApi.exceptions.BusinessLogicException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

@Service
public class GoogleBigQueryApiService {
	private static final String OAUTH_CREDENTIALS_FORMAT = "{ \"client_id\": \"%s\", \"client_secret\": \"%s\", \"refresh_token\": \"%s\", \"grant_type\": \"%s\", \"project_id\": \"%s\", \"dataset_id\": \"%s\" }";

	public List<Map<String, List<String>>> getAllTableNames(OauthCredentials credentials)
			throws BusinessLogicException, ParseException, IOException, InterruptedException {
		String accessToken = this.getAccessToken(credentials);
		List<String> projectIds = getBigQueryProjectIds(accessToken);
		List<Map<String, List<String>>> allTableNamesByDataSetId = new ArrayList<>();

		for (String projectId : projectIds) {
			allTableNamesByDataSetId.add(
					getAllTableNamesAndDataSetNames(accessToken, projectId, getAllDataSetIds(accessToken, projectId)));
		}
		return allTableNamesByDataSetId;
	}

	public List<String> getAllDataSetIds(OauthCredentials credentials)
			throws ParseException, InterruptedException, IOException {
		String accessToken = this.getAccessToken(credentials);
		List<String> projectIds = getBigQueryProjectIds(accessToken);
		List<String> dataSetIds = new ArrayList<>();

		for (String projectId : projectIds) {

			String getDataSetURL = String.format(GoogleBigQueryURLs.BIGQUERY_DATASETS_URL, projectId);

			final HttpGet request = new HttpGet(getDataSetURL);
			request.setHeader("Authorization", "Bearer " + accessToken);

			try (CloseableHttpClient httpClient = HttpClients.createDefault();
					CloseableHttpResponse response = httpClient.execute(request)) {

				int statusCode = response.getCode();

				if (statusCode == 200) {
					String responseString = EntityUtils.toString(response.getEntity());
					JsonObject jsonObject = JsonParser.parseString(responseString).getAsJsonObject();

					JsonArray tablesArray = jsonObject.getAsJsonArray("datasets");

					if (tablesArray != null && !tablesArray.isEmpty()) {
						for (JsonElement dataset : tablesArray) {
							JsonObject datasetObj = dataset.getAsJsonObject();
							JsonObject reference = datasetObj.getAsJsonObject("datasetReference");
							String datasetId = reference.get("datasetId").getAsString();
							dataSetIds.add(datasetId);
						}
					} else {
						System.out.println("No datasets found.");
					}

				}
			} catch (IOException e) {

			}
		}

		return dataSetIds;
	}

	public List<String> getAllDataSetIds(String accessToken, String projectId) throws ParseException {
		List<String> dataSetIds = new ArrayList<>();

		String getDataSetURL = String.format(GoogleBigQueryURLs.BIGQUERY_DATASETS_URL, projectId);

		final HttpGet request = new HttpGet(getDataSetURL);
		request.setHeader("Authorization", "Bearer " + accessToken);

		try (CloseableHttpClient httpClient = HttpClients.createDefault();
				CloseableHttpResponse response = httpClient.execute(request)) {

			int statusCode = response.getCode();

			if (statusCode == 200) {
				String responseString = EntityUtils.toString(response.getEntity());
				JsonObject jsonObject = JsonParser.parseString(responseString).getAsJsonObject();

				JsonArray tablesArray = jsonObject.getAsJsonArray("datasets");

				if (tablesArray != null && !tablesArray.isEmpty()) {
					for (JsonElement dataset : tablesArray) {
						JsonObject datasetObj = dataset.getAsJsonObject();
						JsonObject reference = datasetObj.getAsJsonObject("datasetReference");
						String datasetId = reference.get("datasetId").getAsString();
						dataSetIds.add(datasetId);
					}
				} else {
					System.out.println("No datasets found.");
				}

			}
		} catch (IOException e) {

		}

		return dataSetIds;
	}

	public Map<String, List<String>> getAllTableNamesAndDataSetNames(String accessToken, String projectId,
			List<String> dataSetIds) throws ParseException {
		Map<String, List<String>> dataSetIdAndtableIds = new HashMap<>();

		if (!dataSetIds.isEmpty()) {

			for (String dataSetId : dataSetIds) {
				String getDataSetURL = String.format(GoogleBigQueryURLs.BIGQUERY_TABLE_NAME_URL, projectId, dataSetId);

				final HttpGet request = new HttpGet(getDataSetURL);
				request.setHeader("Authorization", "Bearer " + accessToken);

				try (CloseableHttpClient httpClient = HttpClients.createDefault();
						CloseableHttpResponse response = httpClient.execute(request)) {

					int statusCode = response.getCode();

					if (statusCode == 200) {
						String responseString = EntityUtils.toString(response.getEntity());
						JsonObject jsonObject = JsonParser.parseString(responseString).getAsJsonObject();

						JsonArray tablesArray = jsonObject.getAsJsonArray("tables");

						if (tablesArray != null && !tablesArray.isEmpty()) {
							List<String> tableIds = new ArrayList<>();
							for (JsonElement dataset : tablesArray) {
								JsonObject datasetObj = dataset.getAsJsonObject();
								JsonObject reference = datasetObj.getAsJsonObject("tableReference");
								String tableId = reference.get("tableId").getAsString();
								tableIds.add(tableId);
							}
							dataSetIdAndtableIds.put(dataSetId, tableIds);
						} else {
							System.out.println("No datasets found.");
						}

					}
				} catch (IOException e) {

				}
			}
		}

		return dataSetIdAndtableIds;
	}

	public String getAccessToken(OauthCredentials credentials) throws BusinessLogicException {
		final HttpPost request = new HttpPost(GoogleBigQueryURLs.OAUTH_ACCESS_TOKEN_URL);

		String json = String.format(OAUTH_CREDENTIALS_FORMAT, credentials.getClientId(), credentials.getClientSecret(),
				credentials.getRefreshToken(), credentials.getGrantType(), credentials.getProjectId(),
				credentials.getDatasetId());

		StringEntity entity = new StringEntity(json, ContentType.APPLICATION_JSON);
		request.setEntity(entity);

		try (CloseableHttpClient httpClient = HttpClients.createDefault();
				CloseableHttpResponse response = httpClient.execute(request);) {
			String responseString = EntityUtils.toString(response.getEntity());
			JsonObject responseJson = JsonParser.parseString(responseString).getAsJsonObject();

			return responseJson.get("access_token").getAsString();
		} catch (Exception e) {
			throw new BusinessLogicException("Failed to get access token", e);
		}
	}

	public List<String> getBigQueryProjectIds(String accessToken)
			throws ParseException, InterruptedException, IOException {
		List<String> projectIds = new ArrayList<>();
		int maxRetries = 3;
		int retryDelay = 1000;
		int attempt = 0;

		final HttpGet request = new HttpGet(GoogleBigQueryURLs.GOOGLE_DRIVE_FILES_URL);
		request.setHeader("Authorization", "Bearer " + accessToken);

		try (CloseableHttpClient httpClient = HttpClients.createDefault();
				CloseableHttpResponse response = httpClient.execute(request)) {

			int statusCode = response.getCode();

			if (statusCode == 200) {
				String responseString = EntityUtils.toString(response.getEntity());
				JsonObject jsonObject = JsonParser.parseString(responseString).getAsJsonObject();

				JsonArray tablesArray = jsonObject.getAsJsonArray("projects");

				for (JsonElement tableElement : tablesArray) {
					JsonObject tableObject = tableElement.getAsJsonObject();
					JsonObject tableReference = tableObject.getAsJsonObject("projectReference");
					String tableId = tableReference.get("projectId").getAsString();
					projectIds.add(tableId);
				}
			} else if (statusCode == 429 && attempt < maxRetries) {
				System.out.println("Rate limit exceeded. Retrying in " + retryDelay + "ms...");
				Thread.sleep(retryDelay);
				retryDelay *= 2; // Exponential backoff
				attempt++;
			} else {
				throw new BusinessLogicException("Failed to fetch files. HTTP Status: " + statusCode, statusCode);
			}

		} catch (IOException e) {
			if (attempt == maxRetries)
				throw e; // Rethrow if retries exhausted
			System.out.println("IO error occurred. Retrying in " + retryDelay + "ms...");
			Thread.sleep(retryDelay);
			retryDelay *= 2;
		}
		return projectIds;
	}

}
