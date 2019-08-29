package com.wetrade.ledger_api.states;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import com.wetrade.ledger_api.annotations.DefaultDeserialize;
import com.wetrade.ledger_api.annotations.Deserialize;
import com.wetrade.ledger_api.annotations.Private;
import com.wetrade.ledger_api.annotations.VerifyHash;
import com.wetrade.ledger_api.collections.CollectionRulesHandler;

import org.hyperledger.fabric.contract.execution.JSONTransactionSerializer;
import org.hyperledger.fabric.contract.metadata.TypeSchema;
import org.hyperledger.fabric.contract.routing.TypeRegistry;
import org.hyperledger.fabric.contract.routing.impl.TypeRegistryImpl;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public abstract class State {

    public static Boolean verifyHash(String id, Map<String, byte[]> transientData) {
        throw new RuntimeException("Not yet implemented");
    }

    public static String makeKey(String[] keyParts) {
        return String.join(":", keyParts);
    }

    public static String[] splitKey(String key) {
        return key.split(":");
    }

    public static <T extends State> Boolean verifyHash(Class<T> clazz, Map<String, byte[]> transientData, String hash) {
        // anyway to do this without taking clazz?
        @SuppressWarnings("unchecked")
        Constructor<T>[] constructors = (Constructor<T>[]) clazz.getConstructors();

        for (Constructor<T> constructor : constructors) {
            final VerifyHash annotation = constructor.getAnnotation(VerifyHash.class);

            if (annotation != null) {
                Parameter[] parameters = constructor.getParameters();

                TypeRegistry tr = new TypeRegistryImpl(); // may need some setting up
                JSONTransactionSerializer jts = new JSONTransactionSerializer(tr);

                Object[] args = new Object[parameters.length];

                for (int i = 0; i < parameters.length; i++) {
                    final Parameter param = parameters[i];
                    final String paramName = param.getName();

                    if (!transientData.containsKey(paramName)) {
                        throw new RuntimeException("Transient data missing required property: " + paramName);
                    }

                    TypeSchema schema = TypeSchema.typeConvert(param.getType());
                    args[i] = jts.fromBuffer(transientData.get(paramName), schema);
                }

                T obj = State.buildState(args, constructor);

                return obj.getHash().equals(hash);
            }
        }

        return false;
    }

    public <T extends State> T toPublicForm() {
        String json = this.serialize();
        return (T) State.deserialize(this.getClass(), json, new String[]{});
    }

    public static <T extends State> T deserialize(Class<T> clazz, String json, String[] collections) {
        @SuppressWarnings("unchecked")
        Constructor<T>[] constructors = (Constructor<T>[]) clazz.getConstructors();

        Constructor<T> matchingConstructor = null;

        for (Constructor<T> constructor : constructors) {
            if (collections.length > 0) {
                final Deserialize annotation = constructor.getAnnotation(Deserialize.class);

                if (annotation != null) {
                    CollectionRulesHandler collectionHandler = new CollectionRulesHandler(annotation.collections(), collections);

                    if (collectionHandler.evaluate()) {
                        if (matchingConstructor == null || constructor.getParameterCount() > matchingConstructor.getParameterCount()) {
                            matchingConstructor = constructor;
                        }
                    }
                }
            } else {
                final DefaultDeserialize annotation = constructor.getAnnotation(DefaultDeserialize.class);

                if (annotation != null) {
                    matchingConstructor = constructor;
                    break;
                }
            }
        }

        if (matchingConstructor == null) {
            throw new RuntimeException("No valid constructor found for collections returned");
        }

        JSONObject jsonObject = new JSONObject(json);

        Parameter[] parameters = matchingConstructor.getParameters();

        Object[] args = new Object[parameters.length];

        for (int i = 0; i < parameters.length; i++) {
            final String parameterName = parameters[i].getName();
            final Class<?> parameterType = parameters[i].getType();

            if (!jsonObject.has(parameterName)) {
                throw new JSONException("State missing required constructor argument " + parameterName);
            }

            args[i] = State.resolveJSON(parameterType, jsonObject.get(parameterName), collections);
        }

        return State.buildState(args, matchingConstructor);
    }

    private static <T extends State> Object resolveJSON(Class<?> type, Object value, String[] collections) {
        // TODO does matthews code solve this
        if (State.class.isAssignableFrom(type)) {
            @SuppressWarnings("unchecked")
            Class<T> tClass = (Class<T>) type;
            // value should be a json object in this sense
            return State.deserialize(tClass, value.toString(), collections);
        } else if (type.getName().equals("java.util.Date")) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
            try {
                return formatter.parse((String) value);
            } catch (ParseException e) {
                e.printStackTrace();
                return "";
            }
        } else if (Enum.class.isAssignableFrom(type)) {
            try {
                Method valueOf = type.getMethod("valueOf", String.class);
                return valueOf.invoke(null, value);
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                // should not this this
                e.printStackTrace();
            }
        } else if (type.isArray() || Number.class.isAssignableFrom(type)) {
            final TypeRegistry tr = new TypeRegistryImpl(); // may need some setting up
            final JSONTransactionSerializer jts = new JSONTransactionSerializer(tr);

            final TypeSchema schema = TypeSchema.typeConvert(type);

            String str;

            if (type.isArray()) {
                final JSONArray jsonArray = (JSONArray) value;
                str = jsonArray.toString();
            } else {
                str = ((Number) value).toString();
            }

            return jts.fromBuffer(str.getBytes(), schema);
        }

        return value;
    }

    public static State deserialize(String json) {
        throw new RuntimeException("Not yet implemented");
    };

    private static <T extends State> T buildState(Object[] args, Constructor<T> constructor) {
        try {
            return constructor.newInstance(args);
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e.getMessage());
		}
    }

    private String key;
    @SuppressWarnings("unused")
    private String stateClass;
    private String hash;

    public State(String[] keyParts) {
        this.key = State.makeKey(keyParts);
        this.stateClass = this.getClass().getName();
        this.hash = this.generateHash();
    }

    protected State(String[] keyParts, String hash) {
        this.key = State.makeKey(keyParts);
        this.stateClass = this.getClass().getName();
        this.hash = hash;
    }

    public String serialize() {
        return this.serialize(null);
    }

    public String serialize(String collection) {
        return this.serialize(collection, false);
    }

    private String serialize(String collection, Boolean force) {
        return this.jsonify(collection, force).toString();
    }

    private JSONObject jsonify(String collection, Boolean force) {
        JSONObject json = new JSONObject();

        ArrayList<Field> fields = this.getAllFields();

        for (Field field : fields) {
            field.setAccessible(true);

            if (force || this.shouldAddToJSON(collection, field)) {
                try {
                    Object value = field.get(this);

                    if (value instanceof State) {
                        State stateValue = (State) value;
                        json.put(field.getName(), new JSONObject(stateValue.serialize(collection, force)));
                    } else if (field.getType().getName().equals("java.util.Date") && value != null) {
                        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                        json.put(field.getName(), formatter.format((Date) value));
                    } else {
                        json.put(field.getName(), value);
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }

        return json;
    }

    public String getKey() {
        return this.key;
    }

    public String[] getSplitKey() {
        return State.splitKey(this.key);
    }

    public String getHash() {
        return this.hash;
    }

    private String generateHash() {
        JSONObject jsonObject = this.jsonify(null, true);
        jsonObject.remove(hash);

        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            // SHOULD NEVER HAPPEN BUT CHEERS UP JAVA
            return "";
        }

        byte[] encodedHash = digest.digest(jsonObject.toString().getBytes());

        StringBuilder sb = new StringBuilder();

        for (byte b : encodedHash) {
            sb.append(String.format("%02x", b));
        }

        return sb.toString();
    }

    private ArrayList<Field> getAllFields() {
        @SuppressWarnings("all")
        Class clazz = this.getClass();
        ArrayList<Field> fields = new ArrayList<Field>();

        do {
            fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
        } while ((clazz = clazz.getSuperclass()) != null);

        return fields;
    }

    private boolean shouldAddToJSON(String collection, Field field) {
        if (collection == null) {
            return field.getAnnotation(Private.class) == null;
        }

        final Private annotation = field.getAnnotation(Private.class);

        if (annotation == null) {
            return false;
        }
        CollectionRulesHandler collectionHandler = new CollectionRulesHandler(annotation.collections(), new String[] {collection});

        return collectionHandler.evaluate();
    }
}
