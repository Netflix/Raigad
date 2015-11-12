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
public interface IMembership
{
    /**
     * Get a list of Instances in the current RAC
     */
    public List<String> getRacMembership();

    /**
     * @return Size of current RAC
     */
    public int getRacMembershipSize();

    /**
     * Number of RACs
     */
    public int getRacCount();

    /**
     * Add security group ACLs
     * 
     * @param listIPs
     * @param from
     * @param to
     */
    public void addACL(Collection<String> listIPs, int from, int to);

    /**
     * Remove security group ACLs
     * 
     * @param listIPs
     * @param from
     * @param to
     */
    public void removeACL(Collection<String> listIPs, int from, int to);

    /**
     * List all ACLs
     */
    public List<String> listACL(int from, int to);

    /**
     * Add VPC security group ACLs
     *
     * @param listIPs
     * @param from
     * @param to
     */
    public void addVpcACL(Collection<String> listIPs, int from, int to);

    /**
     * Remove VPC security group ACLs
     *
     * @param listIPs
     * @param from
     * @param to
     */
    public void removeVpcACL(Collection<String> listIPs, int from, int to);

    /**
     * List all VPC ACLs
     */
    public List<String> listVpcACL(int from, int to);
    /**
     * Expand the membership size by 1.
     * 
     * @param count
     */
    public void expandRacMembership(int count);

    /**
     * Return From-To ports for given ACL
     * @param acl
     * @return ACL to Ports Map (From-To)
     * eg. 1.2.3.4 -> 5001,5002
     */
    public Map<String,List<Integer>> getACLPortMap(String acl);
}
