package com.github.davidmoten.bigsort;

import java.util.ArrayList;
import java.util.List;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Producer;
import rx.Subscriber;
import rx.functions.Func1;

public class OperatorRefreshSelect<T> implements OnSubscribe<T> {

	private final Iterable<Observable<T>> sources;
	private final Func1<? extends List<? extends T>, T> selector;

	public OperatorRefreshSelect(Iterable<Observable<T>> sources,
			Func1<? extends List<? extends T>, T> selector) {
		this.sources = sources;
		this.selector = selector;
	}

	@Override
	public void call(final Subscriber<? super T> child) {
		child.setProducer(new MyProducer<T>(sources, selector));
	}

	private static class MyProducer<T> implements Producer {

		private final Iterable<Observable<T>> sources;
		private final List<Subscriber<T>> subscribers;
		private final Func1<? extends List<? extends T>, T> selector;

		public MyProducer(Iterable<Observable<T>> sources,
				Func1<? extends List<? extends T>, T> selector) {
			this.sources = sources;
			this.selector = selector;
			this.subscribers = new ArrayList<Subscriber<T>>();
			for (Observable<T> source : sources) {
				subscribers.add(new SourceSubscriber<T>(source));
			}
		}

		@Override
		public void request(long n) {

		}

	}

	private static class SourceSubscriber<T> extends Subscriber<T> {

		public SourceSubscriber(Observable<T> source) {
		}

		@Override
		public void onCompleted() {
			// TODO Auto-generated method stub

		}

		@Override
		public void onError(Throwable e) {
			// TODO Auto-generated method stub

		}

		@Override
		public void onNext(T t) {
			// TODO Auto-generated method stub

		}

	}

}
