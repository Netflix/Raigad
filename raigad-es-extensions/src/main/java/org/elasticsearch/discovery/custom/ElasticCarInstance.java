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
package org.elasticsearch.discovery.custom;

import java.io.Serializable;

public class ElasticCarInstance implements Serializable
{
	private static final long serialVersionUID = 5606412386974488659L;
	private String hostname;
	private long updatetime;
    private boolean outOfService;

	private String Id;
	private String app;
	private String instanceId;
	private String availabilityZone;
	private String publicip;
	private String dc;
	private String asgName;

	public String getId() {
		return Id;
	}

	public void setId(String id) {
		this.Id = id;
	}

	public String getApp() {
		return app;
	}

	public void setApp(String app) {
		this.app = app;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

	public String getAvailabilityZone() {
		return availabilityZone;
	}

	public void setAvailabilityZone(String availabilityZone) {
		this.availabilityZone = availabilityZone;
	}

	public String getHostName() {
		return hostname;
	}

	public String getHostIP() {
		return publicip;
	}

	public void setHostName(String hostname) {
		this.hostname = hostname;
	}

	public void setHostIP(String publicip) {
		this.publicip = publicip;
	}

	@Override
	public String toString() {
		return String
				.format("Hostname: %s, InstanceId: %s, App: %s, AvailabilityZone : %s, Id : %s, PublicIp : %s, DC : %s, ASG : %s, UpdateTime : %s",
						getHostName(), getInstanceId(), getApp(),
						getAvailabilityZone(), getId(), getHostIP(), getDC(), getAsg(), getUpdatetime());
	}

	public String getDC() {
		return dc;
	}

	public void setDC(String dc) {
		this.dc = dc;
	}

	public String getAsg() {
		return asgName;
	}

	public void setAsg(String asgName) {
		this.asgName = asgName;
	}


	public long getUpdatetime() {
		return updatetime;
	}

	public void setUpdatetime(long updatetime) {
		this.updatetime = updatetime;
	}
	
    public boolean isOutOfService()
    {
        return outOfService;
    }

    public void setOutOfService(boolean outOfService)
    {
        this.outOfService = outOfService;
    }

}
