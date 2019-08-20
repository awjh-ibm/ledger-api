package com.wetrade.ledger_api.handling;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.wetrade.ledger_api.annotations.Private;
import com.wetrade.ledger_api.collections.CollectionRulesHandler;
import com.wetrade.ledger_api.states.State;

import org.hyperledger.fabric.Logger;
import org.hyperledger.fabric.contract.Context;
import org.hyperledger.fabric.shim.ChaincodeStub;
import org.hyperledger.fabric.shim.ledger.KeyValue;
import org.hyperledger.fabric.shim.ledger.QueryResultsIterator;
import org.json.JSONArray;
import org.json.JSONObject;
public class QueryHandler<T extends State> {
    protected final Logger logger = Logger.getLogger(QueryHandler.class);
    private Map<String, JSONObject> collectionQueries;
    private String[] collections;
    private Context ctx;

    public QueryHandler(JSONObject query, String listName, String[] collections, Context ctx, Class<? extends T> supportedClass) {
        this.collections = collections;
        this.ctx = ctx;
        this.collectionQueries = this.parseQuery(query, listName, collections, supportedClass);
    }

    public QueryResponse execute() {
        ArrayList<Map<String, JSONObject>> queryResults = new ArrayList<Map<String, JSONObject>>();

        ChaincodeStub stub = this.ctx.getStub();

        ArrayList<String> usedCollections = new ArrayList<String>();

        final String worldStateQueryString = collectionQueries.get("worldState").getJSONObject("json").toString();
        final QueryResultsIterator<KeyValue> worldStateValues = this.ctx.getStub().getQueryResult(worldStateQueryString);
        queryResults.add(this.iterateIntoMap(worldStateValues));
        
        Set<String> foundIds = queryResults.get(0).keySet();

        for (String collection : this.collections) {
            boolean required = collectionQueries.get(collection).getBoolean("required");
            JSONObject collectionJSON = collectionQueries.get(collection).getJSONObject("json");
            
            // todo update keys and remove those that are already in the map and don't exist here
            String idLimiter = new JSONArray(foundIds).toString();
            
            collectionJSON.getJSONObject("selector").put("_id", new JSONObject("{\"$in\": " + idLimiter + "}"));
            collectionQueries.get(collection).put("json", collectionJSON);

            final String queryString = collectionJSON.toString();
            final QueryResultsIterator<KeyValue> queryResponse = stub.getPrivateDataQueryResult(collection, queryString);
        
            Map<String, JSONObject> queryResult = this.iterateIntoMap(queryResponse);
            if(queryResult.size() == 0) {
                if (required) {
                    return new QueryResponse(new String[] {}, new HashMap<String, JSONObject>());
                }
            } else {
                usedCollections.add(collection);
            }

            foundIds = queryResult.keySet();
            queryResults.add(queryResult);
        }

        Set<String> matchingIds = queryResults.get(0).keySet();

        if (queryResults.size() > 1) {
            for (int i = 1; i < queryResults.size(); i++) {
                Set<String> queryIds = queryResults.get(i).keySet();

                matchingIds.retainAll(queryIds);
            }
        }

        Map<String, JSONObject> finalResult = new HashMap<String, JSONObject>();

        for (String id : matchingIds) {
            for (Map<String, JSONObject> queryResult : queryResults) {
                JSONObject json = queryResult.get(id);

                if (finalResult.containsKey(id)) {
                    final JSONObject existing = finalResult.get(id);

                    for (String jsonKey : JSONObject.getNames(existing)) {
                        json.put(jsonKey, existing.get(jsonKey));
                    }
                }
                
                finalResult.put(id, json);
            }
        }

        return new QueryResponse(usedCollections.toArray(new String[usedCollections.size()]), finalResult);
    }

    private Map<String, JSONObject> iterateIntoMap(QueryResultsIterator<KeyValue> values) {
        Map<String, JSONObject> resultMap = new HashMap<String, JSONObject>();
        
        for (KeyValue value : values) {

            final String data = value.getStringValue();

            JSONObject json = new JSONObject(data);

            resultMap.put(value.getKey(), json);
        }

        return resultMap;
    }

    private Map<String, JSONObject> parseQuery(JSONObject query, String listName, String[] collections, Class<? extends T> clazz) {
        final JSONObject baseQuery = new JSONObject("{\"selector\": {}}");
        baseQuery.getJSONObject("selector").put("_id", new JSONObject());
        baseQuery.getJSONObject("selector").getJSONObject("_id").put("$regex", ".*" + listName + ".*");

        Map<String, JSONObject> collectionQueries = new HashMap<String, JSONObject>();

        collectionQueries.put("worldState", new JSONObject("{\"required\": true, \"json\": " + baseQuery.toString() + "}"));

        for (String collection: collections) {
            collectionQueries.put(collection, new JSONObject("{\"required\": false, \"json\": " + baseQuery.toString() + "}"));
        }

        if (query.has("selector")) {
            JSONObject selector = query.getJSONObject("selector");

            for (String property : selector.keySet()) {
                try {
                    Field field = this.getDeclaredProperty(clazz, property);

                    final Private annotation = field.getAnnotation(Private.class);

                    if (annotation != null) {
                        CollectionRulesHandler collectionHandler = new CollectionRulesHandler(annotation.collections());
                        final String[] entries = collectionHandler.getEntries();
        
                        for (String collection : entries) {
                            JSONObject collectionMapProp = collectionQueries.get(collection);
                            collectionMapProp.put("required", true);
                            JSONObject collectionSelector = collectionMapProp.getJSONObject("json").getJSONObject("selector");
                            collectionSelector.put(property, selector.get(property));
                        }
                    } else {
                        JSONObject worldStateSelector = collectionQueries.get("worldState").getJSONObject("json").getJSONObject("selector");
                        worldStateSelector.put(property, selector.get(property));
                    }

                } catch (NoSuchFieldException | SecurityException e) {
                    throw new RuntimeException("Property " + property + " does not exist for state type " + clazz.getName());
                }
            }
        }

        return collectionQueries;
    }

    private Field getDeclaredProperty(Class<?> clazz, String name) throws NoSuchFieldException {
        try {
            return clazz.getDeclaredField(name);
        } catch (NoSuchFieldException exception) {
            // Ignore
            Class<?> superClazz = clazz.getSuperclass();
            if (superClazz != null) {
                return this.getDeclaredProperty(superClazz, name);
            } else {
                throw exception;
            }
        }
    }
}