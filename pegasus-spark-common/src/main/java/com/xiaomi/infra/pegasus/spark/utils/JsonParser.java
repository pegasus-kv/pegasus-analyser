package com.xiaomi.infra.pegasus.spark.utils;

import com.google.gson.Gson;

public class JsonParser {

    private static final Gson gson = new Gson();

    public static Gson getGson() {
        return gson;
    }
}
