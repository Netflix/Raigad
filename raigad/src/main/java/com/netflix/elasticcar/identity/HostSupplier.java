package com.netflix.elasticcar.identity;

import com.google.common.base.Supplier;
import com.google.inject.ImplementedBy;
import com.netflix.astyanax.connectionpool.Host;

import java.util.List;

@ImplementedBy(EurekaHostsSupplier.class)
public interface HostSupplier {
    public Supplier<List<Host>> getSupplier(String clusterName);
}
