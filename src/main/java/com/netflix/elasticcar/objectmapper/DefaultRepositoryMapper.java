package com.netflix.elasticcar.objectmapper;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.module.SimpleModule;

/**
 * Created by sloke on 7/7/14.
 */
public class DefaultRepositoryMapper extends ObjectMapper
{
    public DefaultRepositoryMapper() {
        this(null);
    }

    public DefaultRepositoryMapper(JsonFactory factory) {
        super(factory);
        SimpleModule serializerModule = new SimpleModule("default serializers", new Version(1, 0, 0, null));
        registerModule(serializerModule);

        configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);

    }
}
