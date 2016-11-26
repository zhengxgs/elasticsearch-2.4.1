package org.zhengxgs;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Created by zhengxgs on 2016/10/28.
 */
public class ElasticsearchClientFactory {

	public Client create(String clusterName, String host, Integer port) throws UnknownHostException {
		Settings settings = Settings.settingsBuilder().put(settings(clusterName)).build();

		TransportClient transportClient = TransportClient
				.builder()
				.settings(settings)
				// .addPlugin(DeleteByQueryPlugin.class)
				.build();

		String[] ips = host.split(",");
		for (String ip : ips) {
			transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), port));
		}
		return transportClient;
	}

	private Map<String, Object> settings(String clusterName) {
		Map<String, Object> settings = new HashMap<>();
		settings.put("cluster.name", clusterName);
		settings.put("client.transport.sniff", "true");
		settings.put("client.transport.ping_timeout", "5s");
		settings.put("client.transport.nodes_sampler_interval", "5s");
		settings.put("client.transport.ignore_cluster_name", "true");
		return settings;
	}
}
