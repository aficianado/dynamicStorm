package com.chaoppo.db.eventhub.mock;

import org.apache.storm.eventhubs.spout.IStateStore;

import java.util.HashMap;
import java.util.Map;

public class StateStoreMock implements IStateStore {

    private static final long serialVersionUID = -7037818324059816380L;
    Map<String, String> myDataMap;

    @Override
    public void open() {
        myDataMap = new HashMap<String, String>();
    }

    @Override
    public void close() {
        myDataMap = null;
    }

    @Override
    public void saveData(String path, String data) {
        if (myDataMap != null) {
            myDataMap.put(path, data);
        }
    }

    @Override
    public String readData(String path) {
        if (myDataMap != null) {
            return myDataMap.get(path);
        }
        return null;
    }
}
