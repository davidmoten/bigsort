package com.github.davidmoten.bigsort;

import rx.Observable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

public class BigSortMain {

    public static void main(String[] args) {
        // for profiling
        int n = 100000;
        System.out.println(n * Math.log(n));
        long t = System.currentTimeMillis();
        TestSubscriber<Object> ts = new TestSubscriber<Object>();
        BigSort.<Integer> sort(Observable.range(1, n).map(i -> n - i + 1), 1000000, 10,
                Schedulers.computation()).subscribe(ts);
        ts.awaitTerminalEvent();
        System.out.println(((System.currentTimeMillis() - t) / 1000.0) + "s");
    }
}
