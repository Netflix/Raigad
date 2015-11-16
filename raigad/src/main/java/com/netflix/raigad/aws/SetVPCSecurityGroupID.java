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
 * This class has been added especially for VPC Purposes.
 * If SecurityGroup is deployed in VPC,
 * then SecurityGroupId is needed to make any modifications or querying to associated SecurityGroup
 *
 * Sets the Security Group Id for the VPC Security Group
 * If SecurityGroupId is not found for the matching the Security Group
 * then RuntimeException is thrown
 * */

@Singleton
public class SetVPCSecurityGroupID
{
    private static final Logger logger = LoggerFactory.getLogger(SetVPCSecurityGroupID.class);
    private final IConfiguration config;
    private final ICredential provider;

    @Inject
    public SetVPCSecurityGroupID(IConfiguration config, ICredential provider)
    {
        this.config = config;
        this.provider = provider;
    }

    public void execute()
    {
        AmazonEC2 client = null;
        try
        {
            client = getEc2Client();

            //Get All the Existing Sec Group Ids
            String[] sec_group_idarr = SystemUtils.getSecurityGroupIds(config.getMacIdForInstance());

            boolean correctSecGroupFound = false;

            for (String sec_grp_id : sec_group_idarr) {

                logger.info("*** " + sec_grp_id);
                DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withGroupIds(sec_grp_id);
                DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);

                for (SecurityGroup secGroup : result.getSecurityGroups()) {
                    logger.info("*** " + secGroup.getGroupName());
                    if (secGroup.getGroupName().equals(config.getACLGroupNameForVPC())) {
                        logger.info("@@@ SecGroupName = " + secGroup.getGroupName() + " matches for given Security Group Id = " + sec_grp_id);
                        correctSecGroupFound = true;
                        //Set the configuration value with Correct Security Group Id
                        config.setACLGroupIdForVPC(sec_grp_id);
                        break;
                    }
                }

                if (correctSecGroupFound)
                    break;
            }
            //If Correct Sec Group is NOT FOUND then throw Exception
            if (!correctSecGroupFound)
                throw new RuntimeException("Sec Group ID and Sec Group Name does NOT match, Something is Wrong, hence failing !!");
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            if (client != null)
                client.shutdown();
        }
    }

    private AmazonEC2 getEc2Client()
    {
        AmazonEC2 client = new AmazonEC2Client(provider.getAwsCredentialProvider());
        client.setEndpoint("ec2." + config.getDC() + ".amazonaws.com");
        return client;
    }
}
