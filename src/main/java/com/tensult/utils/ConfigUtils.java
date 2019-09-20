package com.tensult.utils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tensult.jdbc.types.MultiDatabasesDriverConfig;

public class ConfigUtils {

	public static MultiDatabasesDriverConfig getMultiClusterConfig(String configFilePath) {
		try {
			if (StringUtils.isNotBlank(configFilePath)) {
				File configFile = new File(configFilePath);
				ObjectMapper objectMapper = new ObjectMapper();
				return objectMapper.readValue(configFile, MultiDatabasesDriverConfig.class);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new MultiDatabasesDriverConfig();
	}
	
	public static void main(String args[]) throws IOException {
		Properties topLevelProp = new Properties();
		topLevelProp.put("count", "1000");
		topLevelProp.put("size", "1");
		System.out.println(topLevelProp);
		ObjectMapper objectMapper = new ObjectMapper();
		String propsJson = objectMapper.writeValueAsString(topLevelProp);
		System.out.println(propsJson);
		Properties parsedProps = objectMapper.readValue(propsJson, Properties.class);
		System.out.println(parsedProps);

	}

}
