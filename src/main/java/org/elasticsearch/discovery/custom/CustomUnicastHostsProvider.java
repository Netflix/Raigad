package org.elasticsearch.discovery.custom;

import java.util.List;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastHostsProvider;
import org.elasticsearch.discovery.zen.ping.unicast.UnicastZenPing;
import org.elasticsearch.transport.TransportService;

import com.google.common.collect.Lists;
import com.netflix.elasticcar.IConfiguration;
import com.netflix.elasticcar.identity.ElasticCarInstance;
import com.netflix.elasticcar.identity.IElasticCarInstanceFactory;

public class CustomUnicastHostsProvider extends AbstractComponent implements UnicastHostsProvider {
  private final TransportService transportService;
//  private final IElasticCarInstanceFactory instanceFactory;
  private final Version version;

  @Inject
  IElasticCarInstanceFactory instanceFactory;
  
  @Inject
  IConfiguration config;
  
  @Inject
  public CustomUnicastHostsProvider(Settings settings, TransportService transportService, Version version) {
    super(settings);

    this.transportService = transportService;
//    this.instanceFactory = ElasticSearch.getInjector().getInstance(IElasticCarInstanceFactory.class);
    this.version = version;
  }

  @Override
  public List<DiscoveryNode> buildDynamicNodes() {
    List<DiscoveryNode> discoNodes = Lists.newArrayList();

    // TODO: This should be equal to CassConfig.getCluster due to separate injectors
    List<ElasticCarInstance> instances = instanceFactory.getAllIds(config.getAppName());

    for (ElasticCarInstance instance : instances) {
      try {
        TransportAddress[] addresses = transportService.addressesFromString(instance.getHostIP());
        // we only limit to 1 addresses, makes no sense to ping 100 ports
        for (int i = 0; (i < addresses.length && i < UnicastZenPing.LIMIT_PORTS_COUNT); i++) {
          logger.trace("adding {}, address {}, transport_address {}",
              instance.getId(), instance.getHostIP(), addresses[i]);
          discoNodes.add(new DiscoveryNode(instance.getId(), addresses[i], version));
        }
      } catch (Exception e) {
        logger.warn("failed to add {}, address {}", e, instance.getId(), instance.getHostIP());
      }
    }
    logger.debug("using dynamic discovery nodes {}", discoNodes);

    return discoNodes;
  }
}
