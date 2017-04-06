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

package com.netflix.raigad.discovery;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.UnicastHostsProvider;
import org.elasticsearch.plugins.DiscoveryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public class RaigadDiscoveryPlugin extends Plugin implements DiscoveryPlugin {
    private static final Logger logger = Loggers.getLogger(RaigadDiscoveryPlugin.class);

    private final Settings settings;

    public RaigadDiscoveryPlugin(Settings settings) {
        this.settings = settings;
        logger.info("Starting Raigad custom discovery");
    }

    @Override
    public Map<String, Supplier<UnicastHostsProvider>> getZenHostsProviders(
            TransportService transportService, NetworkService networkService) {
        return Collections.singletonMap(
                "raigad",
                () -> new RaigadUnicastHostsProvider(settings, transportService));
    }
}
