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

import com.google.inject.ImplementedBy;
import com.netflix.raigad.aws.AWSMembership;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Interface to manage membership meta information such as size of RAC, list of
 * nodes in RAC etc. Also perform ACL updates used in multi-regional clusters
 */
@ImplementedBy(AWSMembership.class)
public interface IMembership {
    /**
     * Get a list of instances per RAC
     */
    Map<String, List<String>> getRacMembership(Collection<String> autoScalingGroupNames);

    /**
     * @return Size of current RAC
     */
    int getRacMembershipSize();

    /**
     * Number of RACs
     */
    int getRacCount();

    /**
     * Add security group ACLs
     *
     * @param listIPs
     * @param from
     * @param to
     */
    void addACL(Collection<String> listIPs, int from, int to);

    /**
     * Remove security group ACLs
     *
     * @param listIPs
     * @param from
     * @param to
     */
    void removeACL(Collection<String> listIPs, int from, int to);

    /**
     * List all ACLs
     */
    List<String> listACL(int from, int to);

    /**
     * Expand the membership size by 1
     *
     * @param count
     */
    void expandRacMembership(int count);

    /**
     * Return from-to ports for given ACL
     * @param acl
     * @return ACL to ports map (from-to), eg. 1.2.3.4 -> 5001, 5002
     */
    Map<String, List<Integer>> getACLPortMap(String acl);
}
