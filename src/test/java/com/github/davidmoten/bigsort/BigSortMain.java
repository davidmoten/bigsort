package com.github.davidmoten.bigsort;

import rx.Observable;
import rx.schedulers.Schedulers;

public class BigSortMain {

    public static void main(String[] args) {
        // for profiling
        int n = 10000000;
        BigSort.sort(Observable.range(1, n).map(i -> n - i + 1), 100000, 10, Schedulers.immediate())
                .subscribe();
    }
}
