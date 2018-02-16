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
package com.netflix.raigad.defaultimpl;

import com.google.inject.ImplementedBy;

import java.io.IOException;

/**
 * Interface to aid in starting and stopping Elasticsearch.
 */
@ImplementedBy(ElasticsearchProcessManager.class)
public interface IElasticsearchProcess {
    void start() throws IOException;

    void stop() throws IOException;
}
