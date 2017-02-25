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

package com.netflix.raigad.aws;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.*;
import com.amazonaws.services.autoscaling.model.Instance;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.*;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.identity.IMembership;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class to query amazon ASG for its members to provide - Number of valid nodes
 * in the ASG - Number of zones - Methods for adding ACLs for the nodes
 */
public class AWSMembership implements IMembership {
    private static final Logger logger = LoggerFactory.getLogger(AWSMembership.class);

    private final IConfiguration config;
    private final ICredential provider;

    @Inject
    public AWSMembership(IConfiguration config, ICredential provider) {
        this.config = config;
        this.provider = provider;
    }

    @Override
    public Map<String, List<String>> getRacMembership(Collection<String> autoScalingGroupNames) {
        if (CollectionUtils.isEmpty(autoScalingGroupNames)) {
            return Collections.emptyMap();
        }

        AmazonAutoScaling client = null;

        try {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest describeAutoScalingGroupsRequest =
                    new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(autoScalingGroupNames);
            DescribeAutoScalingGroupsResult describeAutoScalingGroupsResult = client.describeAutoScalingGroups(describeAutoScalingGroupsRequest);

            Map<String, List<String>> asgs = new HashMap<>();
            for (AutoScalingGroup autoScalingGroup : describeAutoScalingGroupsResult.getAutoScalingGroups()) {
                List<String> asgInstanceIds = Lists.newArrayList();
                for (Instance asgInstance : autoScalingGroup.getInstances()) {
                    if (!(asgInstance.getLifecycleState().equalsIgnoreCase("terminating") ||
                            asgInstance.getLifecycleState().equalsIgnoreCase("shutting-down") ||
                            asgInstance.getLifecycleState().equalsIgnoreCase("terminated"))) {
                        asgInstanceIds.add(asgInstance.getInstanceId());
                    }
                }
                asgs.put(autoScalingGroup.getAutoScalingGroupName(), asgInstanceIds);
                logger.info("AWS returned the following instance ID's for {} ASG: {}",
                        autoScalingGroup.getAutoScalingGroupName(),
                        StringUtils.join(asgInstanceIds, ","));
            }

            return asgs;
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /**
     * Actual membership AWS source of truth...
     */
    @Override
    public int getRacMembershipSize() {
        AmazonAutoScaling client = null;

        try {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(config.getASGName());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);

            int size = 0;
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                size += asg.getMaxSize();
            }

            logger.info(String.format("Query on ASG returning %d instances", size));

            return size;
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    @Override
    public int getRacCount() {
        return config.getRacs().size();
    }

    /**
     * Adds a list of IP's to the SG
     */
    public void addACL(Collection<String> listIPs, int from, int to) {
        AmazonEC2 client = null;

        try {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
            ipPermissions.add(new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));

            if (config.isDeployedInVPC()) {
                if (config.getACLGroupIdForVPC().isEmpty()) {
                    throw new RuntimeException("ACLGroupIdForVPC cannot be empty, check if SetVPCSecurityGroupID had any errors");
                }

                client.authorizeSecurityGroupIngress(
                        new AuthorizeSecurityGroupIngressRequest()
                                .withGroupId(config.getACLGroupIdForVPC())
                                .withIpPermissions(ipPermissions));
            } else {
                client.authorizeSecurityGroupIngress(
                        new AuthorizeSecurityGroupIngressRequest(config.getACLGroupName(), ipPermissions));
            }

            logger.info("Added " + StringUtils.join(listIPs, ",") + " to ACL");
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /**
     * Removes a list of IP's from the SG
     */
    public void removeACL(Collection<String> listIPs, int from, int to) {
        AmazonEC2 client = null;

        try {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
            ipPermissions.add(new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));

            if (config.isDeployedInVPC()) {
                if (config.getACLGroupIdForVPC().isEmpty()) {
                    throw new RuntimeException("ACLGroupIdForVPC cannot be empty, check if SetVPCSecurityGroupID had any errors");
                }

                client.revokeSecurityGroupIngress(
                        new RevokeSecurityGroupIngressRequest()
                                .withGroupId(config.getACLGroupIdForVPC())
                                .withIpPermissions(ipPermissions));
            } else {
                client.revokeSecurityGroupIngress(
                        new RevokeSecurityGroupIngressRequest(config.getACLGroupName(), ipPermissions));
            }

            logger.info("Removed " + StringUtils.join(listIPs, ",") + " from ACL");
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /**
     * List SG ACL's
     */
    public List<String> listACL(int from, int to) {
        AmazonEC2 client = null;

        try {
            client = getEc2Client();
            List<String> ipPermissions = new ArrayList<String>();
            DescribeSecurityGroupsResult result;

            if (config.isDeployedInVPC()) {
                if (config.getACLGroupIdForVPC().isEmpty()) {
                    throw new RuntimeException("ACLGroupIdForVPC cannot be empty, check if SetVPCSecurityGroupID had any errors");
                }
                DescribeSecurityGroupsRequest describeSecurityGroupsRequest =
                        new DescribeSecurityGroupsRequest().withGroupIds(config.getACLGroupIdForVPC());
                result = client.describeSecurityGroups(describeSecurityGroupsRequest);
            } else {
                DescribeSecurityGroupsRequest describeSecurityGroupsRequest =
                        new DescribeSecurityGroupsRequest().withGroupNames(Arrays.asList(config.getACLGroupName()));
                result = client.describeSecurityGroups(describeSecurityGroupsRequest);
            }

            for (SecurityGroup group : result.getSecurityGroups()) {
                for (IpPermission perm : group.getIpPermissions()) {
                    if (perm.getFromPort() == from && perm.getToPort() == to) {
                        ipPermissions.addAll(perm.getIpRanges());
                    }
                }
            }

            return ipPermissions;
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    public Map<String, List<Integer>> getACLPortMap(String acl) {
        AmazonEC2 client = null;
        Map<String, List<Integer>> aclPortMap = new HashMap<String, List<Integer>>();

        try {
            client = getEc2Client();
            DescribeSecurityGroupsResult result;

            if (config.isDeployedInVPC()) {
                if (config.getACLGroupIdForVPC().isEmpty()) {
                    throw new RuntimeException("ACLGroupIdForVPC cannot be empty, check if SetVPCSecurityGroupID had any errors");
                }
                DescribeSecurityGroupsRequest describeSecurityGroupsRequest =
                        new DescribeSecurityGroupsRequest().withGroupIds(config.getACLGroupIdForVPC());
                result = client.describeSecurityGroups(describeSecurityGroupsRequest);
            } else {
                DescribeSecurityGroupsRequest describeSecurityGroupsRequest =
                        new DescribeSecurityGroupsRequest().withGroupNames(Arrays.asList(config.getACLGroupName()));
                result = client.describeSecurityGroups(describeSecurityGroupsRequest);
            }

            for (SecurityGroup group : result.getSecurityGroups()) {
                for (IpPermission perm : group.getIpPermissions()) {
                    for (String ipRange : perm.getIpRanges()) {
                        // If given ACL matches from the list of IP ranges then look for "from" and "to" ports
                        if (acl.equalsIgnoreCase(ipRange)) {
                            List<Integer> fromToList = new ArrayList<Integer>();
                            fromToList.add(perm.getFromPort());
                            fromToList.add(perm.getToPort());
                            logger.info("ACL: {}, from: {}, to: {}", acl, perm.getFromPort(), perm.getToPort());
                            aclPortMap.put(acl, fromToList);
                        }
                    }
                }
            }

            return aclPortMap;
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    @Override
    public void expandRacMembership(int count) {
        AmazonAutoScaling client = null;

        try {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(config.getASGName());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);
            AutoScalingGroup asg = res.getAutoScalingGroups().get(0);
            UpdateAutoScalingGroupRequest ureq = new UpdateAutoScalingGroupRequest();
            ureq.setAutoScalingGroupName(asg.getAutoScalingGroupName());
            ureq.setMinSize(asg.getMinSize() + 1);
            ureq.setMaxSize(asg.getMinSize() + 1);
            ureq.setDesiredCapacity(asg.getMinSize() + 1);
            client.updateAutoScalingGroup(ureq);
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    protected AmazonAutoScaling getAutoScalingClient() {
        AmazonAutoScaling client = new AmazonAutoScalingClient(provider.getAwsCredentialProvider());
        client.setEndpoint("autoscaling." + config.getDC() + ".amazonaws.com");
        return client;
    }

    protected AmazonEC2 getEc2Client() {
        AmazonEC2 client = new AmazonEC2Client(provider.getAwsCredentialProvider());
        client.setEndpoint("ec2." + config.getDC() + ".amazonaws.com");
        return client;
    }
}