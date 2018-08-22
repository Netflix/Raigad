package com.netflix.raigad.configuration;

import java.util.List;

/*
Currently, a noop config source
 */
public class CustomConfigSource implements IConfigSource {
    @Override
    public void initialize(String asgName, String region) {

    }

    @Override
    public void initialize(IConfiguration config) {
        //NO OP
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean contains(String key) {
        return false;
    }

    @Override
    public String get(String key) {
        return null;
    }

    @Override
    public String get(String key, String defaultValue) {
        return null;
    }

    @Override
    public boolean get(String key, boolean defaultValue) {
        return false;
    }

    @Override
    public Class<?> get(String key, Class<?> defaultValue) {
        return null;
    }

    @Override
    public <T extends Enum<T>> T get(String key, T defaultValue) {
        return null;
    }

    @Override
    public int get(String key, int defaultValue) {
        return 0;
    }

    @Override
    public long get(String key, long defaultValue) {
        return 0;
    }

    @Override
    public float get(String key, float defaultValue) {
        return 0;
    }

    @Override
    public double get(String key, double defaultValue) {
        return 0;
    }

    @Override
    public List<String> getList(String key) {
        return null;
    }

    @Override
    public List<String> getList(String key, List<String> defaultValue) {
        return null;
    }

    @Override
    public void set(String key, String value) {

    }
}
