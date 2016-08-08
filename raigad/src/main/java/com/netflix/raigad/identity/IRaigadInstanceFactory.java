/**
 * Copyright 2016 Netflix, Inc.
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
package com.netflix.raigad.identity;

import java.util.List;
import java.util.Map;

/**
 *  Interface for managing Elasticsearch instance data.
 *  Provides functionality to register, update, delete or list instances from the registry.
 */

public interface IRaigadInstanceFactory {
    /**
     * Return a list of all Elasticsearch server nodes registered.
     * @param appName the cluster name
     * @return a list of all nodes in {@code appName}
     */
    List<RaigadInstance> getAllIds(String appName);

    /**
     * Return the Elasticsearch server node with the given {@code id}.
     * @param appName the cluster name
     * @param id the node id
     * @return the node with the given {@code id}, or {@code null} if none found
     */
    RaigadInstance getInstance(String appName, String dc, String id);

    /**
     * Create/Register an instance of the server with its info.
     * @param app
     * @param id
     * @param instanceID
     * @param hostname
     * @param ip
     * @param rac
     * @param dc
     * @param asgname
     * @param volumes
     * @return the new node
     */
    RaigadInstance create(String app, String id, String instanceID,
                          String hostname, String ip, String rac, String dc,
                          String asgname, Map<String, Object> volumes);

    /**
     * Delete the server node from the registry
     * @param inst the node to delete
     */
    void delete(RaigadInstance inst);

    /**
     * Update the details of the server node in registry
     * @param inst the node to update
     */
    void update(RaigadInstance inst);

    /**
     * Sort the list by instance ID
     * @param return_ the list of nodes to sort
     */
    void sort(List<RaigadInstance> return_);

    /**
     * Attach volumes if required
     * @param instance
     * @param mountPath
     * @param device
     */
    void attachVolumes(RaigadInstance instance, String mountPath, String device);
}