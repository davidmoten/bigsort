package com.github.davidmoten.rx.operators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;

import com.github.davidmoten.bigsort.BigSort;

public class OperatorResourceMerger<Resource, T> implements Operator<Resource, Resource> {

    private static final Logger log = LoggerFactory.getLogger(OperatorResourceMerger.class);

    private final Comparator<T> comparator;
    private final Func2<Observable<T>, Resource, Observable<Resource>> writer;
    private final Func1<Resource, Observable<T>> reader;
    private final Func0<Resource> resourceFactory;
    private final Action1<Resource> resourceDisposer;
    private final int maxTempResources;

    public OperatorResourceMerger(Comparator<T> comparator,
            Func2<Observable<T>, Resource, Observable<Resource>> writer,
            Func1<Resource, Observable<T>> reader, Func0<Resource> resourceFactory,
            Action1<Resource> resourceDisposer, final int maxTempResources) {
        this.comparator = comparator;
        this.writer = writer;
        this.reader = reader;
        this.resourceFactory = resourceFactory;
        this.resourceDisposer = resourceDisposer;
        this.maxTempResources = maxTempResources;

    }

    @Override
    public Subscriber<? super Resource> call(final Subscriber<? super Resource> child) {
        final List<Resource> resources = new LinkedList<Resource>();
        return new Subscriber<Resource>(child) {

            @Override
            public void onCompleted() {
                reduce();
                for (Resource r : resources)
                    if (!isUnsubscribed())
                        child.onNext(r);
                if (!isUnsubscribed())
                    child.onCompleted();
            }

            @Override
            public void onError(Throwable e) {
                child.onError(e);
            }

            @Override
            public void onNext(Resource r) {
                resources.add(r);
                // log.info("added resource " + r);
                if (resources.size() == maxTempResources) {
                    // log.info("reducing " + resources.size() + " to 1");
                    reduce();
                }
            }

            private void reduce() {
                OperatorResourceMerger.this.reduce(resources, child);
            }
        };
    }

    private void reduce(final List<Resource> resources, final Subscriber<? super Resource> child) {
        if (resources.size() == 1)
            return;
        else {
            Resource resource = resourceFactory.call();
            // log.info("reducing " + resources.size() + " resources");
            Observable<T> items = merge(resources, comparator, reader).doOnCompleted(new Action0() {
                @Override
                public void call() {
                    for (Resource r : resources)
                        resourceDisposer.call(r);
                }
            });
            writer.call(items, resource).subscribe(new Subscriber<Resource>() {

                @Override
                public void onCompleted() {
                    // only emits one so after onNext handled don't care
                }

                @Override
                public void onError(Throwable e) {
                    child.onError(e);
                }

                @Override
                public void onNext(Resource r) {
                    resources.clear();
                    resources.add(r);
                }
            });
        }
    }

    private static <T, Resource> Observable<T> merge(List<Resource> resources,
            final Comparator<T> comparator, final Func1<Resource, Observable<T>> reader) {
        return Observable.just(resources)
        // merge all resources into a single observable stream
                .flatMap(new Func1<List<Resource>, Observable<T>>() {
                    @Override
                    public Observable<T> call(List<Resource> resources) {
                        List<Observable<T>> obs = new ArrayList<Observable<T>>();
                        for (Resource resource : resources)
                            obs.add(reader.call(resource).onBackpressureBuffer());
                        return Observable.create(
                                new OnSubscribeRefreshSelect<T>(obs, BigSort
                                        .<T> minimum(comparator)))
                        // TODO remove this once honours backp
                                .onBackpressureBuffer();
                    }
                });
    }
}
