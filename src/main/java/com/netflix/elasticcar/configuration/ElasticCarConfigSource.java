package com.netflix.elasticcar.configuration;

import com.netflix.elasticcar.configuration.CompositeConfigSource;
import com.netflix.elasticcar.configuration.PropertiesConfigSource;
import com.netflix.elasticcar.configuration.SystemPropertiesConfigSource;

import javax.inject.Inject;


public class ElasticCarConfigSource extends CompositeConfigSource {

    @Inject
    public ElasticCarConfigSource(final PropertiesConfigSource propConfigSource,
                                  final PropertiesConfigSource propertiesConfigSource,
                                  final SystemPropertiesConfigSource systemPropertiesConfigSource) {
        super(propConfigSource, propertiesConfigSource, systemPropertiesConfigSource);
    }
}