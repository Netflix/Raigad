/**
 * Copyright 2017 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.raigad.indexmanagement;

import com.netflix.raigad.objectmapper.DefaultIndexMapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.List;

public class IndexUtils {

    /**
     * Convert the JSON String of parameters to IndexMetadata objects
     *
     * @param serializedIndexMetadata : JSON string with parameters
     * @return list of IndexMetadata objects
     * @throws IOException
     */
    public static List<IndexMetadata> parseIndexMetadata(String serializedIndexMetadata) throws IOException {
        ObjectMapper jsonMapper = new DefaultIndexMapper();
        TypeReference<List<IndexMetadata>> typeRef = new TypeReference<List<IndexMetadata>>() {};
        return jsonMapper.readValue(serializedIndexMetadata, typeRef);
    }
}
