package com.github.davidmoten.rx.operators;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;

import com.github.davidmoten.bigsort.BigSort;

public class OperatorResourceMerger<Resource, T> implements Operator<Resource, Resource> {

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
        return new ResourcesSubscriber<Resource, T>(child, comparator, writer, reader,
                resourceFactory, resourceDisposer, maxTempResources);
    }

    private static class ResourcesSubscriber<Resource, T> extends Subscriber<Resource> {

        final List<Resource> resources = new LinkedList<Resource>();
        private final Subscriber<? super Resource> child;
        private final int maxTempResources;
        private final Comparator<T> comparator;
        private final Func2<Observable<T>, Resource, Observable<Resource>> writer;
        private final Func1<Resource, Observable<T>> reader;
        private final Func0<Resource> resourceFactory;
        private final Action1<Resource> resourceDisposer;

        public ResourcesSubscriber(Subscriber<? super Resource> child, Comparator<T> comparator,
                Func2<Observable<T>, Resource, Observable<Resource>> writer,
                Func1<Resource, Observable<T>> reader, Func0<Resource> resourceFactory,
                Action1<Resource> resourceDisposer, int maxTempResources) {
            super(child);
            this.child = child;
            this.comparator = comparator;
            this.writer = writer;
            this.reader = reader;
            this.resourceFactory = resourceFactory;
            this.resourceDisposer = resourceDisposer;
            this.maxTempResources = maxTempResources;
        }

        void requestMore(long n) {
            request(n);
        }

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
            } else
                request(1);
        }

        private void reduce() {
            reduce(resources, child);
        }

        private void reduce(final List<Resource> resources, final Subscriber<? super Resource> child) {
            if (resources.size() == 1)
                return;
            else {
                Resource resource = resourceFactory.call();
                // log.info("reducing " + resources.size() + " resources");
                Observable<T> items = merge(resources, comparator, reader).doOnTerminate(() -> {
                    for (Resource r : resources)
                        resourceDisposer.call(r);
                });
                writer.call(items, resource).unsafeSubscribe(new Subscriber<Resource>() {

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

    }

    private static <T, Resource> Observable<T> merge(List<Resource> resourcesList,
            final Comparator<T> comparator, final Func1<Resource, Observable<T>> reader) {
        return Observable.just(resourcesList)
        // merge all resources into a single observable stream
                .flatMap(
                        resources -> {
                            List<Observable<T>> obs = new ArrayList<>();
                            for (Resource resource : resources)
                                obs.add(reader.call(resource).onBackpressureBuffer());
                            return Observable.create(
                                    new OnSubscribeRefreshSelect<T>(obs, BigSort
                                            .minimum(comparator)))
                            // add backpressure support to RefeshSelect operator
                                    .onBackpressureBuffer();
                        });
    }
}
