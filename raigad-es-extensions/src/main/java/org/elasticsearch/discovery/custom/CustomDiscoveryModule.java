package org.elasticsearch.discovery.custom;

import org.elasticsearch.discovery.Discovery;
import org.elasticsearch.discovery.zen.ZenDiscoveryModule;

public class CustomDiscoveryModule extends ZenDiscoveryModule {
  @Override
  protected void bindDiscovery() {
    bind(Discovery.class).to(CustomDiscovery.class).asEagerSingleton();
  }
}
