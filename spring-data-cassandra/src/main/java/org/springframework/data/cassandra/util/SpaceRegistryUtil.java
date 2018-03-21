package org.springframework.data.cassandra.util;


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.data.cassandra.core.CassandraTemplate;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class SpaceRegistryUtil {
    private final static String SUBDOMAIN_NAME = "polyglotSubdomainName";
    private final static String STRUCTURE_NAME = "polyglotStructureName";
    private final static String TENANT_ID = "tenantId";
    private static Map<String,CassandraTemplate> cassandraSessionCacheMap = new HashMap<>();

    public static <S> String getSpaceId(S entity) {

        String json = getJSONString(entity);
        Map<String, Object> requestMap = getMapObject(json);
        return getTenantToken(requestMap);
    }

    public static String getJSONString(Object object) {

        String jsonString = null;
        ObjectMapper objMapper = new ObjectMapper();
        try {
            jsonString = objMapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return jsonString;
    }

    public static Map<String, Object> getMapObject(String value) {

        Map<String, Object> persistenceDataMap = null;
        ObjectMapper objMapper = new ObjectMapper();

        objMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
        objMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        objMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try {
            persistenceDataMap = objMapper.readValue(value, LinkedHashMap.class);
        } catch (Exception e) {
            e.printStackTrace();
        }


        return persistenceDataMap;
    }

    private static String getTenantToken(Map<String, Object> payloadMap) {
        return (String) payloadMap.get(TENANT_ID);
    }

    public static void registerTenant(String tenantId){
        Cluster.Builder builder = Cluster.builder();
        builder.addContactPoints("127.0.0.1");
        builder.withPort(9042);
        Cluster cluster = builder.build();
        Session session = cluster.connect("mykeyspace");
        String query = "CREATE KEYSPACE "+ tenantId +" WITH replication "
                + "= {'class':'SimpleStrategy', 'replication_factor':1}; ";
        session.execute(query);
        session.close();

    }

    public CassandraTemplate getSession(Object entity){

        String spaceId = getSpaceId(entity);
        CassandraTemplate template = this.getCassandraSessionCacheMap(spaceId);
        if(template == null){
            template = createCassandraTemplate(spaceId);
            setCassandraSessionCacheMap(spaceId,template);
        }
        return template;
    }

    public static CassandraTemplate getCassandraSessionCacheMap(String sessionKey) {
        return cassandraSessionCacheMap.get(sessionKey);
    }

    public static void setCassandraSessionCacheMap(String sessionKey,CassandraTemplate cassandraTemplate) {

        if(cassandraSessionCacheMap.get(sessionKey)==null){
            cassandraSessionCacheMap.put(sessionKey,cassandraTemplate);
        }

    }

    private CassandraTemplate createCassandraTemplate(String spaceId){
        Cluster.Builder builder = Cluster.builder();
        builder.addContactPoints("127.0.0.1");
        builder.withPort(9042);
        Cluster cluster = builder.build();
        Session session = cluster.connect(spaceId);
        return new CassandraTemplate(session);
    }
}