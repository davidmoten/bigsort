package com.github.davidmoten.bigsort;

import rx.functions.Action1;

public class Actions {

    public static interface Throws1<T> {
        void call(T t) throws Exception;
    }

    public static <T> Action1<T> re(Throws1<T> thr) {
        return t -> {
            try {
                thr.call(t);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }
}
