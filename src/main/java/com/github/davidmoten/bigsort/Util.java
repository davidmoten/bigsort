package com.github.davidmoten.bigsort;

import rx.Observable;
import rx.functions.Func1;

public class Util {

    public static <Resource> Func1<Resource, Observable<Resource>> nested() {
        return r -> Observable.just(r);
    }
}
