package com.netflix.elasticcar.indexmanagement;

/**
 * Courtesy Jae Bae
 */
public interface IIndexNameFilter {

    public boolean filter(String name);
    public String getNamePart(String name);
    public String getId();
}
