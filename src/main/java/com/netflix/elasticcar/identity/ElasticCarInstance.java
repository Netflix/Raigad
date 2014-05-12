/**
 * Copyright 2013 Netflix, Inc.
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
package com.netflix.elasticcar.identity;

import java.io.Serializable;
import java.util.Map;

public class ElasticCarInstance implements Serializable
{
	private static final long serialVersionUID = 5606412386974488659L;
	private String hostname;
	private long updatetime;

	private String Id;
	private String app;
	private String instanceId;
	private String availabilityZone;
	private String publicip;
	private String dc;

	public String getId() {
		return Id;
	}

	public void setId(String id) {
		Id = id;
	}

	public String getApp() {
		return app;
	}

	public ElasticCarInstance setApp(String app) {
		this.app = app;
		return this;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public ElasticCarInstance setInstanceId(String instanceId) {
		this.instanceId = instanceId;
		return this;
	}

	public String getAvailabilityZone() {
		return availabilityZone;
	}

	public ElasticCarInstance setAvailabilityZone(String availabilityZone) {
		this.availabilityZone = availabilityZone;
		return this;
	}

	public String getHostName() {
		return hostname;
	}

	public String getHostIP() {
		return publicip;
	}

	public ElasticCarInstance setHostName(String hostname) {
		this.hostname = hostname;
		return this;
	}

	public ElasticCarInstance setHostIP(String publicip) {
		this.publicip = publicip;
		return this;
	}

	@Override
	public String toString() {
		return String
				.format("Hostname: %s, InstanceId: %s, App: %s, Availability Zone : %s, Location %s",
						getHostName(), getInstanceId(), getApp(),
						getAvailabilityZone(), getDC());
	}

	public String getDC() {
		return dc;
	}

	public ElasticCarInstance setDC(String dc) {
		this.dc = dc;
		return this;
	}

	public long getUpdatetime() {
		return updatetime;
	}

	public void setUpdatetime(long updatetime) {
		this.updatetime = updatetime;
	}
}
