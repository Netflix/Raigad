package com.netflix.elasticcar.identity;

import com.google.common.base.Supplier;
import com.netflix.astyanax.connectionpool.Host;

import java.util.List;

public interface HostSupplier {
    public Supplier<List<Host>> getSupplier(String clusterName);
}
