package com.netflix.raigad.utils;

import com.google.inject.ImplementedBy;
import com.netflix.raigad.defaultimpl.StandardTuner;

import java.io.IOException;

@ImplementedBy(StandardTuner.class)
public interface IElasticsearchTuner {
    void writeAllProperties(String yamlLocation, String hostname) throws IOException;
}
