package com.github.davidmoten.bigsort;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Producer;
import rx.Subscriber;
import rx.functions.Func1;
import rx.internal.operators.NotificationLite;

import com.github.davidmoten.util.Optional;

public class OnSubscribeRefreshSelect<T> implements OnSubscribe<T> {

	private final Iterable<Observable<T>> sources;
	private final Func1<List<T>, Integer> selector;

	public OnSubscribeRefreshSelect(Iterable<Observable<T>> sources,
			Func1<List<T>, Integer> selector) {
		this.sources = sources;
		this.selector = selector;
	}

	@Override
	public void call(final Subscriber<? super T> child) {
		child.setProducer(new MyProducer<T>(sources, selector, child));
	}

	private static class MyProducer<T> implements Producer {

		private NotificationLite<T> on = NotificationLite.instance();

		private final List<SourceSubscriber<T>> subscribers;
		private final Func1<List<T>, Integer> selector;
		private final Subscriber<? super T> child;
		private final AtomicLong expected = new AtomicLong();
		private final Deque<Object> queue = new LinkedList<Object>();
		private Object lock;
		private final AtomicBoolean firstTime = new AtomicBoolean(true);

		public MyProducer(Iterable<Observable<T>> sources,
				Func1<List<T>, Integer> selector, Subscriber<? super T> child) {
			this.selector = selector;
			this.child = child;
			this.subscribers = new ArrayList<SourceSubscriber<T>>();

			int i = 0;
			for (Observable<T> source : sources) {
				SourceSubscriber<T> subscriber = new SourceSubscriber<T>(this,
						i);
				subscribers.add(subscriber);
				source.subscribe(subscriber);
				i++;
			}

		}

		@Override
		public void request(long n) {
			if (n <= 0)
				return;

			if (firstTime.compareAndSet(true, false)) {
				for (SourceSubscriber<T> subscriber : subscribers)
					subscriber.requestMore(1);
			}
			if (expected.get() == Long.MAX_VALUE) {
				if (n == Long.MAX_VALUE)
					expected.set(n);
				else {
					while (true) {
						// lock free updater
						long current = expected.get();
						long next = current + n;
						// check for addition past MAX_VALUE
						if (next < 0)
							next = Long.MAX_VALUE;
						if (expected.compareAndSet(current, next))
							break;
					}
				}
			}
			synchronized (lock) {
				drainQueue();
			}
		}

		public void onCompleted(int index) {
			synchronized (lock) {
				emitAndRequest();
			}
		}

		public void onError(Throwable e, int index) {
			synchronized (lock) {
				queue.add(on.error(e));
				drainQueue();
			}
		}

		public void onNext(T t, int index) {
			synchronized (lock) {
				emitAndRequest();
			}
		}

		private static class IndexValue<T> {
			int index;
			T value;

			IndexValue(int index, T value) {
				this.index = index;
				this.value = value;
			}
		}

		private void emitAndRequest() {

			if (countPresent(subscribers) >= countActive(subscribers)) {
				List<IndexValue<T>> indexValues = indexValues(subscribers);
				List<T> values = values(indexValues);
				int i = selector.call(values);
				// find which subscriber reported the value
				int index = indexValues.get(i).index;
				// emit the value
				queue.add(on.next(values.get(i)));
				// mark value as used and request more
				SourceSubscriber<T> subscriber = subscribers.get(index);
				subscriber.markUsed();
				drainQueue();
				if (!subscriber.isCompleted())
					subscribers.get(i).requestMore(1);
			}
		}

		private List<IndexValue<T>> indexValues(
				List<SourceSubscriber<T>> subscribers) {
			List<IndexValue<T>> list = new ArrayList<IndexValue<T>>(
					subscribers.size());
			for (int i = 0; i < subscribers.size(); i++) {
				Optional<T> latest = subscribers.get(i).latest();
				if (latest.isPresent() && !subscribers.get(i).used())
					list.add(new IndexValue<T>(i, latest.get()));
			}
			return list;
		}

		private static <T> List<T> values(List<IndexValue<T>> indexValues) {
			List<T> list = new ArrayList<T>(indexValues.size());
			for (IndexValue<T> iv : indexValues)
				list.add(iv.value);
			return list;
		}

		private static <T> int countPresent(
				List<SourceSubscriber<T>> subscribers) {
			int count = 0;
			for (SourceSubscriber<T> subscriber : subscribers)
				if (subscriber.latest().isPresent())
					count++;
			return count;
		}

		private static <T> int countActive(List<SourceSubscriber<T>> subscribers) {
			int count = 0;
			for (SourceSubscriber<T> subscriber : subscribers)
				if (!subscriber.isCompleted() && !subscriber.used())
					count++;
			return count;
		}

		private void drainQueue() {
			while (true) {
				Object item = queue.peek();
				if (item == null || child.isUnsubscribed())
					break;
				else if (on.isCompleted(item) || on.isError(item)) {
					on.accept(child, queue.poll());
					break;
				} else if (expected.get() == 0)
					break;
				else {
					// expected won't be Long.MAX_VALUE so can safely
					// decrement
					if (expected.get() != Long.MAX_VALUE)
						expected.decrementAndGet();
					on.accept(child, queue.poll());
				}
			}
		}

	}

	private static class SourceSubscriber<T> extends Subscriber<T> {

		private final MyProducer<T> producer;
		private final int index;
		private volatile boolean completed = false;
		private volatile Optional<T> latest = Optional.absent();
		// latest has been used
		private volatile boolean used = false;

		SourceSubscriber(MyProducer<T> producer, int index) {
			this.producer = producer;
			this.index = index;
		}

		void requestMore(long n) {
			request(n);
		}

		Optional<T> latest() {
			return latest;
		}

		boolean used() {
			return used;
		}

		void markUsed() {
			used = true;
		}

		boolean isCompleted() {
			return completed;
		}

		@Override
		public void onStart() {
			// don't request any yet
			request(0);
		}

		@Override
		public void onCompleted() {
			completed = true;
			producer.onCompleted(index);
		}

		@Override
		public void onError(Throwable e) {
			producer.onError(e, index);

		}

		@Override
		public void onNext(T t) {
			latest = Optional.of(t);
			used = false;
			producer.onNext(t, index);
		}

	}

}
