package com.wetrade.ledger_api.states;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Map;

import com.wetrade.ledger_api.handling.QueryHandler;
import com.wetrade.ledger_api.handling.QueryResponse;

import org.hyperledger.fabric.Logger;
import org.hyperledger.fabric.contract.Context;
import org.hyperledger.fabric.shim.ledger.KeyModification;
import org.hyperledger.fabric.shim.ledger.KeyValue;
import org.hyperledger.fabric.shim.ledger.QueryResultsIterator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public abstract class StateList<T extends State> {
    private Logger logger = Logger.getLogger(StateList.class);
    private String name;
    private Class<? extends T> supportedClass;
    private String[] collections;
    private Context ctx;

    public StateList(Context ctx, String listName) {
        this.ctx = ctx;
        this.name = listName + "|";
        this.supportedClass = null;

        Map<String, byte[]> trans = ctx.getStub().getTransient();

        try {
            String stringCollection = new String(trans.get("collections"));
            this.collections = new JSONArray(stringCollection).toList().toArray(new String[]{});
        } catch (Exception ex) {
            logger.warning("No private data collections provided");
            this.collections = new String[]{};
        }
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
                    logger.info("Collection being added to: " + collection);
                    this.ctx.getStub().putPrivateData(collection, key, privateData);
                } catch (Exception err) {
                    // TODO CHECK IF THIS HAPPENS AS NOT ALLOWED OR BECAUSE OTHER BAD THINGS HAVE HAPPENED
                }
            }
        }
    }

    public void addBulk(T[] states) throws RuntimeException {
        for (T state : states) {
            this.add(state);
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
        if (!this.supportedClass.getName().equals(stateClass)) {
            throw new RuntimeException("Cannot get state for key " + key + ". State class is not in list of supported classes for state list.");
        }

        final Class<? extends T> clazz = this.supportedClass;

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

    public T getByHash(String hash) {
        JSONObject hashQuery = new JSONObject("{\"selector\": {\"hash\": \""+hash+"\"}}");
        ArrayList<T> assets = this.query(hashQuery);
        if (assets.size() > 1) {
            throw new RuntimeException("More than one asset shares the same hash...");
        } else if (assets.size() == 0) {
            throw new RuntimeException("There are no " + this.name + " with this hash");
        }
        return assets.get(0);
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
        final QueryHandler<T> qh = new QueryHandler<T>(query, this.name, this.collections, this.ctx, this.supportedClass);
        final QueryResponse queryResult = qh.execute();

        final String[] usedCollections = queryResult.getUsedCollections();
        final Map<String, JSONObject> queryResultMap = queryResult.getQueryResult();

        ArrayList<T> queryResultArray = new ArrayList<T>();

        for (Map.Entry<String, JSONObject> result : queryResultMap.entrySet()) {
            T state;
            try {
                JSONObject mapProp = result.getValue();

                state = this.deserialize(mapProp, usedCollections);
                queryResultArray.add(state);
            } catch (RuntimeException err) {
                err.printStackTrace();
                throw new RuntimeException("Failed to run query. " + err.getMessage());
            }
        }

        return queryResultArray;
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

    // TODO: Remove useless param
    private String[] getCollections(Class clazz) {
        return this.collections;
    }

    protected void use(Class<? extends T> stateClass) {
        this.supportedClass = stateClass;

        this.collections = this.getCollections(stateClass);
    }

    private T deserialize(JSONObject json, String[] collections) {
        // final Class<? extends T> clazz = this.supportedClasses.get(stateClass);
        final Class<? extends T> clazz = this.supportedClass;

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
