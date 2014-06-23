package com.netflix.elasticcar.defaultimpl;

import javax.inject.Inject;

import com.netflix.elasticcar.CompositeConfigSource;
import com.netflix.elasticcar.PropertiesConfigSource;
import com.netflix.elasticcar.SystemPropertiesConfigSource;


public class ElasticCarConfigSource extends CompositeConfigSource {

    @Inject
    public ElasticCarConfigSource(final PropertiesConfigSource simpleDBConfigSource,
                                  final PropertiesConfigSource propertiesConfigSource,
                                  final SystemPropertiesConfigSource systemPropertiesConfigSource) {
        super(simpleDBConfigSource, propertiesConfigSource, systemPropertiesConfigSource);
    }
}