package com.chaoppo.db.storm.error;

public class StormAdapterException extends Exception {

    private static final long serialVersionUID = 3347816866745709739L;

    public StormAdapterException(Throwable t) {
        super(t);
    }

    public StormAdapterException(String msg) {
        super(msg);
    }

    public StormAdapterException(String msg, Throwable t) {
        super(msg, t);
    }

}
