package com.netflix.elasticcar.utils;


public class NFException {

	private final String cfKey;
    private final String pathName;
    private final String stacktrace;
    
    public NFException(String cfKey,String pathName,String stacktrace)
    {
    		this.cfKey = cfKey;
    		this.pathName = pathName;
    		this.stacktrace = stacktrace;
    }
    
    public String getCfKey() {
		return cfKey;
	}

	public String getPathName() {
		return pathName;
	}

	public String getStacktrace() {
		return stacktrace;
	}

}
