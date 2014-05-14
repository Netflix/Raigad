package org.elasticsearch.discovery.cass;

import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.ZenDiscoveryModule;

public class CassDiscoveryModule extends ZenDiscoveryModule {
  @Override
  protected void bindDiscovery() {
    bind(Discovery.class).to(CassDiscovery.class).asEagerSingleton();
  }
}
