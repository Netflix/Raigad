package com.netflix.elasticcar.identity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.google.common.collect.ImmutableMap;

public abstract class MasterQuorum {
  protected static ESLogger log = Loggers.getLogger(MasterQuorum.class);

  private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

  public static AtomicInteger quorumCount = new AtomicInteger();

  protected abstract boolean delegate();
  protected abstract int discoverySize();
  protected abstract void updateSecuritySettings();

  private final ClusterAdminClient client;

  public MasterQuorum(ClusterAdminClient client) {
    this.client = client;
  }

  public void setMasterQuorum() {
    try {
      if (delegate()) {
        final int newQuorumCount = (discoverySize() / 2 + 1);
        if (quorumCount.get() != newQuorumCount) {
          quorumCount.set(newQuorumCount);

          ClusterUpdateSettingsRequest clusterUpdateSettingsRequest = Requests.clusterUpdateSettingsRequest();
          clusterUpdateSettingsRequest.listenerThreaded(false);
          clusterUpdateSettingsRequest.persistentSettings(
              ImmutableMap.<String, Object>builder()
                  .put("discovery.zen.minimum_master_nodes", quorumCount)
                  .build());
          client.updateSettings(
              clusterUpdateSettingsRequest,
              new ActionListener<ClusterUpdateSettingsResponse>() {
                @Override
                public void onResponse(ClusterUpdateSettingsResponse response) {
                  try {
                    log.info("master quorum set as " + quorumCount);
                  } catch (Throwable e) {
                    onFailure(e);
                  }
                }

                @Override
                public void onFailure(Throwable e) {
                  log.error("failed to handle cluster state", e);
                }
              });
        }
      }
    } catch (Exception e) {
      log.error("Exception on setting master quorum: " + e.getMessage(), e);
    }
  }

  @PostConstruct
  public void start() {
    scheduler.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        try {
          setMasterQuorum();
          updateSecuritySettings();
        } catch (Exception e) {
          log.error("Exception on setting master quorum: " + e.getMessage(), e);
        }
      }
    }, 120, 120, TimeUnit.SECONDS);
  }

  @PreDestroy
  public void shutdown() {
    scheduler.shutdownNow();
  }
}
