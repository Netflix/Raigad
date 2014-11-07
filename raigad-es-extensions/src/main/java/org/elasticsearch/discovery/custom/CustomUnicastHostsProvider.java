/**
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elasticsearch.discovery.custom;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.transport.TransportService;

import java.util.List;


public class CustomUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {
	private final TransportService transportService;
	private final Version version;
  
  @Inject
  public CustomUnicastHostsProvider(Settings settings, TransportService transportService, Version version) {
    super(settings);
    this.transportService = transportService;
    this.version = version;
  }

  @Override
  public List<DiscoveryNode> buildDynamicNodes() {
		List<DiscoveryNode> discoNodes = Lists.newArrayList();
		try {
		String strNodes = DataFetcher.fetchData("http://127.0.0.1:8080/Raigad/REST/v1/esconfig/get_nodes",logger);
		List<RaigadInstance> instances = ElasticsearchUtil.getRaigadInstancesFromJsonString(strNodes, logger);
			for (RaigadInstance instance : instances) {
				try {
					TransportAddress[] addresses = transportService.addressesFromString(instance.getHostIP());
					// we only limit to 1 addresses, makes no sense to ping 100 ports
					for (int i = 0; (i < addresses.length && i < UnicastZenPing.LIMIT_PORTS_COUNT); i++) {
						logger.debug(
								"adding {}, address {}, transport_address {}",
								instance.getId(), instance.getHostIP(),addresses[i]);
						discoNodes.add(new DiscoveryNode(instance.getId(),addresses[i], version));
					}
				} catch (Exception e) {
					logger.warn("failed to add {}, address {}", e,instance.getId(), instance.getHostIP());
				}
			}
		} catch (Exception e) {
			logger.error("Caught an exception while trying to add buildDynamicNodes", e);
			throw new RuntimeException(e);
		}
    logger.info("using dynamic discovery nodes {}", discoNodes);

    return discoNodes;
  }
}
