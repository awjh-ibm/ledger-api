package com.wetrade.ledger_api.states;

import java.lang.reflect.Constructor;
import java.lang.reflect.Parameter;

import com.wetrade.ledger_api.annotations.DefaultDeserialize;
import com.wetrade.ledger_api.states.utils.Deserializer;
import com.wetrade.ledger_api.states.utils.Serializer;

public class Concept {
    public static <T extends Concept> T deserialize(Class<T> clazz, String json) {
        return Deserializer.deserialize(clazz, json, null);
    }

    public String serialize() {
        return Serializer.serialize(this, null, false);
    }
}