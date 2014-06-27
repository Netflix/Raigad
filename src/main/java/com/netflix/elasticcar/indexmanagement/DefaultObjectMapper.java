package com.netflix.elasticcar.indexmanagement;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.module.SimpleModule;

/**
 * Courtesy: Jae Bae
 */
public class DefaultObjectMapper extends ObjectMapper {
    public DefaultObjectMapper() {
        this(null);
    }

    public DefaultObjectMapper(JsonFactory factory) {
        super(factory);
        SimpleModule serializerModule = new SimpleModule("default serializers", new Version(1, 0, 0, null));
        registerModule(serializerModule);

        configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        configure(SerializationConfig.Feature.AUTO_DETECT_GETTERS, false);
        configure(SerializationConfig.Feature.AUTO_DETECT_FIELDS, false);
        configure(SerializationConfig.Feature.INDENT_OUTPUT, false);
    }
}
