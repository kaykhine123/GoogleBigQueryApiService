package com.GoogleBigQueryApi.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hc.core5.http.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.GoogleBigQueryApi.constants.AppConstants;
import com.GoogleBigQueryApi.domain.GoogleBigQueryApiService;
import com.GoogleBigQueryApi.domain.entity.OauthCredentials;
import com.GoogleBigQueryApi.exceptions.BusinessLogicException;

@RestController
public class GoogleBigQueryApiController {
	OauthCredentials credentials = new OauthCredentials(AppConstants.CLIENT_ID, AppConstants.CLIENT_SECRET,
			AppConstants.REFRESH_TOKEN, AppConstants.PROJECT_ID, AppConstants.DATASET_ID);

	@Autowired
	GoogleBigQueryApiService gbqApiService;

	@GetMapping("/tableNamesAndDataSetIds")
	public List<Map<String, List<String>>> getAllDataSetIdstableNames()
			throws BusinessLogicException, ParseException, IOException, InterruptedException {
		List<Map<String, List<String>>> DataSetIdsAndtableNames = gbqApiService.getAllTableNames(credentials);
		return DataSetIdsAndtableNames;
	}

	@GetMapping("/dataSetIds")
	public List<String> getAllDataSetIds()
			throws BusinessLogicException, ParseException, IOException, InterruptedException {
		List<String> DataSetIds = gbqApiService.getAllDataSetIds(credentials);
		return DataSetIds;
	}

}
