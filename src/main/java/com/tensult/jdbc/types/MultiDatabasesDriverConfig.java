package com.tensult.jdbc.types;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections4.MapUtils;

public class MultiDatabasesDriverConfig {
	
	private Map<String, Properties> connectionsInfo;
	
	private String connectionChooserScript;
	
	private String queryRewriterScript;
	
	public Map<String, Properties> getConnectionUrls() {
		return connectionsInfo;
	}

	public void setConnectionUrls(Map<String, Properties> connectionInfo) {
		this.connectionsInfo = connectionInfo;
	}

	public String getConnectionChooserScript() {
		return connectionChooserScript;
	}

	public void setConnectionChooserScript(String connectionChooserScript) {
		this.connectionChooserScript = connectionChooserScript;
	}

	public String getQueryRewriterScript() {
		return queryRewriterScript;
	}

	public void setQueryRewriterScript(String queryRewriterScript) {
		this.queryRewriterScript = queryRewriterScript;
	}
	
	public Properties getConnectionInfo(String connectionId) {
		return MapUtils.getObject(connectionsInfo, connectionId);
	}
	
	public void putConnectionInfo(String connectionId, Properties connectionInfo) {
		if(this.connectionsInfo == null) {
			this.connectionsInfo = new HashMap<String, Properties>();
		}
		this.connectionsInfo.put(connectionId, connectionInfo);
	}

	@Override
	public String toString() {
		return "MultiClustersDriverConfig [connectionUrls=" + connectionsInfo + ", connectionChooserScript="
				+ connectionChooserScript + ", queryRewriterScript=" + queryRewriterScript + "]";
	}

}
