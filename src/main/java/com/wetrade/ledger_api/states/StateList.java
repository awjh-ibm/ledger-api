package com.wetrade.ledger_api.states;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.wetrade.ledger_api.annotations.Private;
import com.wetrade.ledger_api.collections.CollectionRulesHandler;

import org.hyperledger.fabric.contract.Context;
import org.hyperledger.fabric.shim.ledger.KeyModification;
import org.hyperledger.fabric.shim.ledger.KeyValue;
import org.hyperledger.fabric.shim.ledger.QueryResultsIterator;
import org.json.JSONException;
import org.json.JSONObject;

public abstract class StateList<T extends State> {
    private String name;
    private Map<String, Class<? extends T>> supportedClasses;
    private ArrayList<String> collections;
    private Context ctx;

    public StateList(Context ctx, String listName) {
        this.ctx = ctx;
        this.name = listName;
        this.supportedClasses = new HashMap<String, Class<? extends T>>();
        this.collections = new ArrayList<String>();
    }

    public boolean exists(String key) {
        try {
            this.getWorldStateData(key);
            return true;
        } catch (Exception err) {
            return false;
        }
    }

    public void add(T state) throws RuntimeException {
        final String stateKey = state.getKey();

        if (this.exists(stateKey)) {
            throw new RuntimeException("Cannot add state. State already exists for key " + stateKey);
        }

        final String key = this.ctx.getStub().createCompositeKey(this.name, state.getSplitKey()).toString();

        final String serialized = state.serialize();
        final byte[] worldStateData = serialized.getBytes();

        this.ctx.getStub().putState(key, worldStateData);

        for (String collection : this.getCollections(state.getClass())) {
            final String collectionSerialized = state.serialize(collection);
            final byte[] privateData = collectionSerialized.getBytes();

            if (privateData.length > 2) {
                try {
                    this.ctx.getStub().putPrivateData(collection, key, privateData);
                } catch (Exception err) {
                    // TODO CHECK IF THIS HAPPENS AS NOT ALLOWED OR BECAUSE OTHER BAD THINGS HAVE HAPPENED
                }
            }
        }
    }

    private String getWorldStateData(String key) {
        final String ledgerKey = this.ctx.getStub().createCompositeKey(this.name, State.splitKey(key)).toString();
        final String worldStateData = new String(this.ctx.getStub().getState(ledgerKey));

        if (worldStateData.length() == 0) {
            throw new RuntimeException("Cannot get state. No state exists for key " + key);
        }

        return worldStateData;
    }

    public T get(String key) throws RuntimeException {
        final String ledgerKey = this.ctx.getStub().createCompositeKey(this.name, State.splitKey(key)).toString();
        final String worldStateData = this.getWorldStateData(key);

        JSONObject stateJSON = new JSONObject(worldStateData);
        String stateClass = stateJSON.getString("stateClass");
        if (!this.supportedClasses.containsKey(stateClass)) {
            throw new RuntimeException("Cannot get state for key " + key + ". State class is not in list of supported classes for state list.");
        }

        final Class<? extends T> clazz = this.supportedClasses.get(stateClass);

        ArrayList<String> usedCollections = new ArrayList<String>();

        for (String collection : this.getCollections(clazz)) {
            try {
                final String privateData = new String(ctx.getStub().getPrivateData(collection, ledgerKey));

                if (privateData.length() > 0) {
                    JSONObject privateJSON = new JSONObject(privateData);

                    for (String jsonKey : JSONObject.getNames(privateJSON)) {
                        stateJSON.put(jsonKey, privateJSON.get(jsonKey));
                    }

                    usedCollections.add(collection);
                }
            } catch (Exception err) {
                // ignore
            }
        }

        T returnVal;

        try {
            returnVal = this.deserialize(stateJSON, usedCollections.toArray(new String[usedCollections.size()]));
        } catch (Exception err) {
            throw new RuntimeException("Failed to deserialize " + key + ". " + err.getMessage());
        }
        return returnVal;
    }

    @SuppressWarnings("unchecked")
    public HistoricState<T>[] getHistory(String key) {
        // No history for private data
        final String ledgerKey = this.ctx.getStub().createCompositeKey(this.name, State.splitKey(key)).toString();
        final QueryResultsIterator<KeyModification> keyHistory = this.ctx.getStub().getHistoryForKey(ledgerKey);

        ArrayList<HistoricState<T>> hsArrList = new ArrayList<HistoricState<T>>();

        for (KeyModification modification : keyHistory) {
            final String worldStateData = modification.getStringValue();

            JSONObject worldStateJSON = new JSONObject(worldStateData);

            T state;
            try {
                state = this.deserialize(worldStateJSON, new String[] {});
            } catch (RuntimeException err) {
                throw new RuntimeException("Failed to get history for key " + key + ". " + err.getMessage());
            }

            final Long ts = modification.getTimestamp().toEpochMilli();
            final String txId = modification.getTxId();

            final HistoricState<T> hs = new HistoricState<T>(ts, txId, state);

            hsArrList.add(hs);
        }

        HistoricState<T>[] hsArr = hsArrList.toArray(new HistoricState[hsArrList.size()]);

        return hsArr;
    }

    public ArrayList<T> query(JSONObject query) {
        final JSONObject baseQuery = new JSONObject("{\"selector\": {}}");
        baseQuery.getJSONObject("selector").put("_id", new JSONObject());
        baseQuery.getJSONObject("selector").getJSONObject("_id").put("$regex", ".*" +  this.name + ".*");

        Map<String, JSONObject> collectionQueries = new HashMap<String, JSONObject>();
        collectionQueries.put("worldstate", new JSONObject("{\"required\": true, \"json\": " + baseQuery.toString() + "}"));

        for (String collection: this.collections) {
            collectionQueries.put(collection, new JSONObject("{\"required\": false, \"json\": " + baseQuery.toString() + "}"));
        }

        if (query.has("selector")) {
            // TODO add refinement for handling all couchdb formats
            // TODO make it handle when sub properties may be further private
            // TODO tidy into smaller functions
            // TODO explore how to do an or over multiple tables as for now requires a value to be returned by all the necessary tables
            JSONObject selector = query.getJSONObject("selector");

            for (String property : selector.keySet()) {
                for (Entry<String, Class<? extends T>> entry : this.supportedClasses.entrySet()) {
                    Class<? extends T> clazz = entry.getValue();

                    try {
                        Field field = clazz.getDeclaredField(property);
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
                            JSONObject worldStateSelector = collectionQueries.get("worldstate").getJSONObject("json").getJSONObject("selector");
                            worldStateSelector.put(property, selector.get(property));
                        }

                    } catch (NoSuchFieldException | SecurityException e) {
                        throw new RuntimeException("Property " + property + " does not exist for state type " + entry.getValue().getName());
                    }
                }
            }
        }

        int requiredCollections = 0;

        for (Entry<String, JSONObject> entry : collectionQueries.entrySet()) {
            if (entry.getValue().getBoolean("required")) {
                requiredCollections++;
            }
        }

        // TODO IF HAS SELECTOR LOOK UP WHETHER THE PROPERTY FOR THAT SELECTOR HAS A PRIVATE ANNOTATION, QUERY AGAINST COLLECTIONS WITH THAT THEN GRAB REST OF THE DATA FROM OTHER COLLECTIONS

        Map<String, JSONObject> valuesArrMap = new HashMap<String, JSONObject>();

        java.util.function.Predicate<QueryResultsIterator<KeyValue>> iterate = (values) -> {
            boolean used = false;

            for (KeyValue value : values) {
                used = true;

                final String data = value.getStringValue();

                JSONObject json = new JSONObject(data);
                int count = 1;

                JSONObject mapProp = new JSONObject();

                if (valuesArrMap.containsKey(value.getKey())) {
                    final JSONObject existing = valuesArrMap.get(value.getKey());
                    int existingCount = existing.getInt("collectionCount");
                    final JSONObject existingJSON = existing.getJSONObject("json");

                    for (String jsonKey : JSONObject.getNames(existingJSON)) {
                        json.put(jsonKey, existingJSON.get(jsonKey));
                    }

                    count = existingCount + 1;
                }

                mapProp.put("json", json);
                mapProp.put("collectionCount", count);

                valuesArrMap.put(value.getKey(), mapProp);
            }

            return used;
        };

        final QueryResultsIterator<KeyValue> worldStateValues = this.ctx.getStub().getQueryResult(collectionQueries.get("worldstate").getJSONObject("json").toString());
        iterate.test(worldStateValues);

        ArrayList<String> usedCollections = new ArrayList<String>();

        for (String collection : this.collections) {
            try {
                final String queryString = collectionQueries.get(collection).getJSONObject("json").toString();
                final QueryResultsIterator<KeyValue> privateValues = this.ctx.getStub().getPrivateDataQueryResult(collection, queryString);
                if (iterate.test(privateValues)) {
                    usedCollections.add(collection);
                }
            } catch (Exception err) {
                err.printStackTrace();
                // can't use that store
            }
        }

        // T[] valuesArr = (T[]) new State[valuesArrMap.size()];
        ArrayList<T> valuesArr = new ArrayList<T>();

        for (Map.Entry<String, JSONObject> result : valuesArrMap.entrySet()) {
            T state;
            try {
                JSONObject mapProp = result.getValue();

                if (mapProp.getInt("collectionCount") >= requiredCollections) {
                    state = this.deserialize(mapProp.getJSONObject("json"), usedCollections.toArray(new String[usedCollections.size()]));
                    valuesArr.add(state);
                }
            } catch (RuntimeException err) {
                err.printStackTrace();
                throw new RuntimeException("Failed to run query. " + err.getMessage());
            }
        }

        return valuesArr;
    }

    public ArrayList<T> getAll() {
        return this.query(new JSONObject());
    }

    @SuppressWarnings("unused")
    public int count() {
        final QueryResultsIterator<KeyValue> values = this.ctx.getStub().getStateByPartialCompositeKey(this.name);

        int counter = 0;
        for (KeyValue ignore : values) {
            counter++;
        }

        return counter;
    }

    public void update(T state) {
        this.update(state, false);
    }

    public void update(T state, boolean force) throws RuntimeException {
        final String stateKey = state.getKey();

        if (!this.exists(stateKey) && !force) {
            throw new RuntimeException("Cannot update state. No state exists for key " + stateKey);
        }

        final String ledgerKey = this.ctx.getStub().createCompositeKey(this.name, state.getSplitKey()).toString();

        final byte[] data = state.serialize().getBytes();

        this.ctx.getStub().putState(ledgerKey, data);

        for (String collection : this.getCollections(state.getClass())) {
            final byte[] privateData = state.serialize(collection).getBytes();

            if (privateData.length > 2) {
                try {
                    this.ctx.getStub().putPrivateData(collection, ledgerKey, privateData);
                } catch (Exception err) {
                    // can't access that store
                }
            }
        }
    }

    public void delete(String key) {
        if (this.exists(key)) {
            final T state = this.get(key);

            final String ledgerKey = this.ctx.getStub().createCompositeKey(this.name, State.splitKey(key)).toString();

            this.ctx.getStub().delState(ledgerKey);

            for (String collection : this.getCollections(state.getClass())) {
                try {
                    this.ctx.getStub().delPrivateData(collection, ledgerKey);
                } catch (Exception err) {
                    // can't access that store
                }
            }
        }
    }

    @SuppressWarnings("rawtypes")
    private String[] getCollections(Class clazz) {
        // don't want to do this everytime. make more efficient
        final ArrayList<String> collections = new ArrayList<String>();

        for (Field field : clazz.getDeclaredFields()) {
            final Private annotation = field.getAnnotation(Private.class);

            if (annotation != null) {
                CollectionRulesHandler collectionHandler = new CollectionRulesHandler(annotation.collections());
                final String[] entries = collectionHandler.getEntries();

                collections.addAll(Arrays.asList(entries));
            }
        }

        return Arrays.stream(collections.toArray(new String[collections.size()])).distinct().toArray(String[]::new);
    }

    protected void use(Class<? extends T> stateClass) {
        this.supportedClasses.put(stateClass.getName(), stateClass);

        String[] collections = this.getCollections(stateClass);

        for (String collection : collections ) {
            if (!this.collections.contains(collection)) {
                this.collections.add(collection);
            }
        }
    }

    protected void use(Class<? extends T>[] stateClasses) {
        for (Class<? extends T> stateClass : stateClasses) {
            this.use(stateClass);
        }
    }

    private T deserialize(JSONObject json, String[] collections) {
        String stateClass = json.getString("stateClass");

        final Class<? extends T> clazz = this.supportedClasses.get(stateClass);

        try {
            return State.deserialize(clazz, json.toString(), collections);
        } catch (JSONException e) {
            throw new RuntimeException("Failed to deserialize. " + e.getMessage());
        } catch (RuntimeException e) {
            if (e.getMessage().equals("No valid constructor found for collections returned")) {
                return this.deserialize(json, clazz);
            }
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private T deserialize(JSONObject json, Class<? extends T> clazz) {
        Method deserialize;
        try {
            deserialize = clazz.getMethod("deserialize", String.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("State class missing deserialize function" + e.getMessage());
        }

        T state;
        try {
            state = (T) deserialize.invoke(null, json.toString());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return state;
    }
}
