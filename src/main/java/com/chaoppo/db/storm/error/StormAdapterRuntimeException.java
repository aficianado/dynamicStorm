package com.chaoppo.db.storm.error;

public class StormAdapterRuntimeException extends RuntimeException {

    private static final long serialVersionUID = -8570191948586196532L;

    public StormAdapterRuntimeException(Throwable t) {
        super(t);
    }

    public StormAdapterRuntimeException(String msg) {
        super(msg);
    }

    public StormAdapterRuntimeException(String msg, Throwable t) {
        super(msg, t);
    }

}
