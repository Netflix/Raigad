/**
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.raigad.objectmapper;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.module.SimpleModule;

public class DefaultMasterNodeInfoMapper extends ObjectMapper {
    public DefaultMasterNodeInfoMapper() {
        this(null);
    }

    public DefaultMasterNodeInfoMapper(JsonFactory factory) {
        super(factory);
        SimpleModule serializerModule = new SimpleModule("default serializers", new Version(1, 0, 0, null));
        registerModule(serializerModule);

        configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        configure(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS, false);
    }
}
