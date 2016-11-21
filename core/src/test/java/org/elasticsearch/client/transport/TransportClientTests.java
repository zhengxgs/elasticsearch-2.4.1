/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.transport;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class TransportClientTests extends ESTestCase {

    public void testPluginTypesError() {
        try {
            TransportClient.Builder builder = TransportClient.builder();
            builder.settings(Settings.builder().put("plugin.types", "BogusPlugin").build());
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("plugin.types is no longer supported"));
        }
    }


    // TODO 大爷
    public static void main(String[] args) throws UnknownHostException {
        TransportClientTests.create("elasticsearch-2.4", "172.16.4.33", 9300);
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static Client create(String clusterName, String host, Integer port) throws UnknownHostException {
        Settings settings = Settings.settingsBuilder().put(settings(clusterName)).build();
        TransportClient transportClient = TransportClient
            .builder()
            .settings(settings)
            .build();

        String[] ips = host.split(",");
        for (String ip : ips) {
            transportClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ip), port));
        }
        return transportClient;
    }

    private static Map<String, Object> settings(String clusterName) {
        Map<String, Object> settings = new HashMap<>();
        settings.put("cluster.name", clusterName);
        settings.put("client.transport.sniff", "true");
        settings.put("client.transport.ping_timeout", "5s");
        settings.put("client.transport.nodes_sampler_interval", "5s");
        settings.put("client.transport.ignore_cluster_name", "false");
        return settings;
    }
}
