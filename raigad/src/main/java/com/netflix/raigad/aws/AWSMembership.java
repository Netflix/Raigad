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
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class to query amazon ASG for its members to provide - Number of valid nodes
 * in the ASG - Number of zones - Methods for adding ACLs for the nodes
 */
public class AWSMembership implements IMembership
{
    private static final Logger logger = LoggerFactory.getLogger(AWSMembership.class);
    private final IConfiguration config;
    private final ICredential provider;    

    @Inject
    public AWSMembership(IConfiguration config, ICredential provider)
    {
        this.config = config;
        this.provider = provider;        
    }

    @Override
    public List<String> getRacMembership()
    {
        AmazonAutoScaling client = null;
        try
        {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(config.getASGName());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);

            List<String> instanceIds = Lists.newArrayList();
            for (AutoScalingGroup asg : res.getAutoScalingGroups())
            {
                for (Instance ins : asg.getInstances())
                    if (!(ins.getLifecycleState().equalsIgnoreCase("Terminating") || ins.getLifecycleState().equalsIgnoreCase("shutting-down") || ins.getLifecycleState()
                            .equalsIgnoreCase("Terminated")))
                        instanceIds.add(ins.getInstanceId());
            }
            logger.info(String.format("Querying Amazon returned following instance in the ASG: %s --> %s", config.getRac(), StringUtils.join(instanceIds, ",")));
            return instanceIds;
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    /**
     * Actual membership AWS source of truth...
     */
    @Override
    public int getRacMembershipSize()
    {
        AmazonAutoScaling client = null;
        try
        {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest().withAutoScalingGroupNames(config.getASGName());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);
            int size = 0;
            for (AutoScalingGroup asg : res.getAutoScalingGroups())
            {
                size += asg.getMaxSize();
            }
            logger.info(String.format("Query on ASG returning %d instances", size));
            return size;
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    @Override
    public int getRacCount()
    {
        return config.getRacs().size();
    }

    /**
     * Adds a iplist to the SG.
     */
    public void addACL(Collection<String> listIPs, int from, int to)
    {
        AmazonEC2 client = null;
        try
        {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
            ipPermissions.add(new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));
            client.authorizeSecurityGroupIngress(new AuthorizeSecurityGroupIngressRequest(config.getACLGroupName(), ipPermissions));
            logger.info("Done adding ACL to: " + StringUtils.join(listIPs, ","));
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    /**
     * removes a iplist from the SG
     */
    public void removeACL(Collection<String> listIPs, int from, int to)
    {
        AmazonEC2 client = null;
        try
        {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
            ipPermissions.add(new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));
            client.revokeSecurityGroupIngress(new RevokeSecurityGroupIngressRequest(config.getACLGroupName(), ipPermissions));
            logger.info("Done removing from ACL: " + StringUtils.join(listIPs, ","));
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    /**
     * List SG ACL's
     */
    public List<String> listACL(int from, int to)
    {
        AmazonEC2 client = null;
        try
        {
            client = getEc2Client();
            List<String> ipPermissions = new ArrayList<String>();
            DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withGroupNames(Arrays.asList(config.getACLGroupName()));
            DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
            for (SecurityGroup group : result.getSecurityGroups())
            {
                for (IpPermission perm : group.getIpPermissions())
                {
                    if (perm.getFromPort() == from && perm.getToPort() == to)
                    {
                   		ipPermissions.addAll(perm.getIpRanges());
                    }
                }
            }
            return ipPermissions;
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    /**
     * Adds a iplist to the VPC SG.
     */
    public void addVpcACL(Collection<String> listIPs, int from, int to)
    {
        AmazonEC2 client = null;
        try
        {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
            ipPermissions.add(new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));
            client.authorizeSecurityGroupIngress(new AuthorizeSecurityGroupIngressRequest(config.getACLGroupNameForVPC(), ipPermissions));
            logger.info("Done adding VPC ACL to: " + StringUtils.join(listIPs, ","));
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    /**
     * removes a iplist from the VPC SG
     */
    public void removeVpcACL(Collection<String> listIPs, int from, int to)
    {
        AmazonEC2 client = null;
        try
        {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<IpPermission>();
            ipPermissions.add(new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));
            client.revokeSecurityGroupIngress(new RevokeSecurityGroupIngressRequest(config.getACLGroupNameForVPC(), ipPermissions));
            logger.info("Done removing from VPC ACL: " + StringUtils.join(listIPs, ","));
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    /**
     * List VPC SG ACL's
     */
    public List<String> listVpcACL(int from, int to)
    {
        AmazonEC2 client = null;
        try
        {
            client = getEc2Client();
            List<String> ipPermissions = new ArrayList<String>();
            DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withGroupNames(Arrays.asList(config.getACLGroupNameForVPC()));
            DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
            for (SecurityGroup group : result.getSecurityGroups())
            {
                for (IpPermission perm : group.getIpPermissions())
                {
                    if (perm.getFromPort() == from && perm.getToPort() == to)
                    {
                        ipPermissions.addAll(perm.getIpRanges());
                    }
                }
            }
            return ipPermissions;
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    public Map<String,List<Integer>> getACLPortMap(String acl)
    {
        AmazonEC2 client = null;
        Map<String,List<Integer>> aclPortMap = new HashMap<String,List<Integer>>();
        try
        {
            client = getEc2Client();
            DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withGroupNames(Arrays.asList(config.getACLGroupName()));
            DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
            for (SecurityGroup group : result.getSecurityGroups())
            {
                for (IpPermission perm : group.getIpPermissions())
                {
                    for(String ipRange : perm.getIpRanges())
                    {
                        //If given ACL matches from the list of ipRanges
                        //then look for From and To Ports
                        if(acl.equalsIgnoreCase(ipRange))
                        {
                            List<Integer> fromToList = new ArrayList<Integer>();
                            fromToList.add(perm.getFromPort());
                            fromToList.add(perm.getToPort());
                            logger.info("ACL = {}, From = {}, To = {}",acl,perm.getFromPort(),perm.getToPort());
                            aclPortMap.put(acl,fromToList);
                        }
                    }
                }
            }
            return aclPortMap;
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    @Override
    public void expandRacMembership(int count)
    {
        AmazonAutoScaling client = null;
        try
        {
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
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    protected AmazonAutoScaling getAutoScalingClient()
    {
        AmazonAutoScaling client = new AmazonAutoScalingClient(provider.getAwsCredentialProvider());
        client.setEndpoint("autoscaling." + config.getDC() + ".amazonaws.com");
        return client;
    }

    protected AmazonEC2 getEc2Client()
    {
        AmazonEC2 client = new AmazonEC2Client(provider.getAwsCredentialProvider());
        client.setEndpoint("ec2." + config.getDC() + ".amazonaws.com");
        return client;
    }
}
