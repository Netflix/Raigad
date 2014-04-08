package com.netflix.elasticcar;

import java.io.IOException;

import com.google.inject.ImplementedBy;
import com.netflix.elasticcar.defaultimpl.ElasticSearchProcessManager;

/**
 * Interface to aid in starting and stopping Elasticsearch.
 *
 */
@ImplementedBy(ElasticSearchProcessManager.class)
public interface IElasticsearchProcess
{
    void start(boolean join_ring) throws IOException;

    void stop() throws IOException;
}
