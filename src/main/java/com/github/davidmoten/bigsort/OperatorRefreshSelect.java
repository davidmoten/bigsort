package com.github.davidmoten.bigsort;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Producer;
import rx.Subscriber;
import rx.functions.Func1;
import rx.internal.operators.NotificationLite;

import com.github.davidmoten.util.Optional;

public class OperatorRefreshSelect<T> implements OnSubscribe<T> {

	private final Iterable<Observable<T>> sources;
	private final Func1<List<Optional<T>>, Integer> selector;

	public OperatorRefreshSelect(Iterable<Observable<T>> sources,
			Func1<List<Optional<T>>, Integer> selector) {
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
		private final Func1<List<Optional<T>>, Integer> selector;
		private volatile List<Optional<T>> values;
		private final AtomicInteger active = new AtomicInteger();
		private final Subscriber<? super T> child;
		private final AtomicLong expected = new AtomicLong();
		private final Deque<Object> queue = new LinkedList<Object>();
		private Object lock;

		public MyProducer(Iterable<Observable<T>> sources,
				Func1<List<Optional<T>>, Integer> selector,
				Subscriber<? super T> child) {
			this.selector = selector;
			this.child = child;
			this.subscribers = new ArrayList<SourceSubscriber<T>>();
			this.values = new ArrayList<Optional<T>>();

			int i = 0;
			for (Observable<T> source : sources) {
				SourceSubscriber<T> subscriber = new SourceSubscriber<T>(this,
						i);
				values.add(Optional.<T> absent());
				subscribers.add(subscriber);
				active.incrementAndGet();
				source.subscribe(subscriber);
				i++;
			}
			for (SourceSubscriber<T> ss : subscribers) {
				ss.requestMore(1);
			}
		}

		@Override
		public void request(long n) {
			if (n <= 0 || expected.get() == Long.MAX_VALUE)
				return;
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

		public void onCompleted(int index) {
			synchronized (lock) {
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
				values.set(index, Optional.of(t));
				emitAndRequest();
			}
		}

		private void emitAndRequest() {
			if (countPresent(values) >= active.get()) {
				int i = selector.call(values);
				if (values.get(i).isPresent())
					queue.add(on.next(values.get(i).get()));
				drainQueue();

				// now remove from list and request
				values.set(i, Optional.<T> absent());
				subscribers.get(i).requestMore(1);
			}
		}

		private static <T> int countPresent(Collection<Optional<T>> list) {
			int count = 0;
			for (Optional<T> op : list)
				if (op.isPresent())
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

		SourceSubscriber(MyProducer<T> producer, int index) {
			this.producer = producer;
			this.index = index;
		}

		void requestMore(long n) {
			request(n);
		}

		boolean isCompleted() {
			return completed;
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
			producer.onNext(t, index);
		}

	}

}
