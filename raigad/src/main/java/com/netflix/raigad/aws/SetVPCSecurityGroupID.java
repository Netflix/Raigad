package com.netflix.raigad.aws;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.raigad.configuration.IConfiguration;
import com.netflix.raigad.utils.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sloke on 11/16/15.
 * This class has been added especially for VPC Purposes. If SecurityGroup is deployed in VPC,
 * then SecurityGroupId is needed to make any modifications or querying to associated SecurityGroup
 *
 * Sets the Security Group Id for the VPC Security Group
 * If SecurityGroupId is not found for the matching the Security Group
 * then RuntimeException is thrown
 * */

@Singleton
public class SetVPCSecurityGroupID {
    private static final Logger logger = LoggerFactory.getLogger(SetVPCSecurityGroupID.class);
    private final IConfiguration config;
    private final ICredential provider;

    @Inject
    public SetVPCSecurityGroupID(IConfiguration config, ICredential provider) {
        this.config = config;
        this.provider = provider;
    }

    public void execute() {
        AmazonEC2 client = null;

        try {
            client = getEc2Client();

            //Get All the Existing Sec Group Ids
            String[] securityGroupIds = SystemUtils.getSecurityGroupIds(config.getMacIdForInstance());
            DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withGroupIds(securityGroupIds);
            DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);

            boolean securityGroupFound = false;

            for (SecurityGroup securityGroup : result.getSecurityGroups()) {
                logger.info("Read " + securityGroup.getGroupName());

                if (securityGroup.getGroupName().equals(config.getACLGroupNameForVPC())) {
                    logger.info("Found matching security group name: " + securityGroup.getGroupName());

                    // Setting configuration value with the correct SG ID
                    config.setACLGroupIdForVPC(securityGroup.getGroupId());
                    securityGroupFound = true;

                    break;
                }
            }

            // If correct SG was not found, throw Exception
            if (!securityGroupFound) {
                throw new RuntimeException("Cannot find matching security group for " + config.getACLGroupNameForVPC());
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    private AmazonEC2 getEc2Client() {
        AmazonEC2 client = new AmazonEC2Client(provider.getAwsCredentialProvider());
        client.setEndpoint("ec2." + config.getDC() + ".amazonaws.com");
        return client;
    }
}
