package com.custardsource.hbase;

import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.FactoryBean;

/**
 * <p>
 * Applications that must connect to different HBase clusters depending on their environment can use
 * this class to paramaterize the {@link HBaseConfiguration} that is created.
 * </p>
 * <p>
 * The default {@link HBaseConfiguration} is created, and a set of mutations to it are applied based
 * on the configured {@link Map} of properties
 * </p>
 * <p>
 * General Use cases are for an application that needs both a dev and production modes, where uner
 * Dev, you wish to use a local non-distributed cluster, but once packaged up, this application may
 * need to connect to a real life cluster, using a Spring configuration lifecycle.
 * </p>
 */
public class HBaseConfigurationFactory implements FactoryBean {

    private final HBaseConfiguration hbaseConfiguration;

    public HBaseConfigurationFactory() {
        this.hbaseConfiguration = new HBaseConfiguration();
    }

    public HBaseConfigurationFactory(Map<String, String> propertyMap) {
        this();
        for (Map.Entry<String, String> entry : propertyMap.entrySet()) {
            hbaseConfiguration.set(entry.getKey(), entry.getValue());
        }

    }

    @Override
    public Object getObject() throws Exception {
        return hbaseConfiguration;
    }

    @Override
    public Class<? extends HBaseConfiguration> getObjectType() {
        return HBaseConfiguration.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

}
