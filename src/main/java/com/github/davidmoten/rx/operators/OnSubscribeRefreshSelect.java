package com.github.davidmoten.rx.operators;

import static com.github.davidmoten.util.Optional.of;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceArray;

import rx.Notification;
import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Producer;
import rx.Scheduler.Worker;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Func1;
import rx.internal.operators.NotificationLite;
import rx.schedulers.Schedulers;

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
		private final NotificationLite<T> on = NotificationLite.instance();

		private final List<SourceSubscriber<T>> subscribers;
		private final Func1<List<T>, Integer> selector;
		private final Subscriber<? super T> child;
		private final AtomicLong expected = new AtomicLong();
		private final AtomicBoolean firstTime = new AtomicBoolean(true);
		private final AtomicReferenceArray<SubscriberStatus<T>> status;
		private final Worker worker;
		private final AtomicInteger nextRequestFrom = new AtomicInteger(-1);

		public MyProducer(Iterable<Observable<T>> sources,
				Func1<List<T>, Integer> selector, Subscriber<? super T> child) {
			this.selector = selector;
			this.child = child;
			this.worker = Schedulers.trampoline().createWorker();
			this.subscribers = new ArrayList<SourceSubscriber<T>>();

			{
				int i = 0;
				for (Observable<T> source : sources) {
					SourceSubscriber<T> subscriber = new SourceSubscriber<T>(
							this, i);
					subscribers.add(subscriber);
					// nothing should be started by the subscriber because
					// onStart
					// requests 0
					source.subscribe(subscriber);
					i++;
				}
			}
			status = new AtomicReferenceArray<SubscriberStatus<T>>(
					subscribers.size());
			for (int i = 0; i < subscribers.size(); i++) {
				status.set(i, new SubscriberStatus<T>(Optional.<T> absent(),
						false, false));
			}
		}

		private static class SubscriberStatus<T> {
			final Optional<T> latest;
			final boolean completed;
			final boolean used;

			SubscriberStatus(Optional<T> latest, boolean completed, boolean used) {
				this.latest = latest;
				this.completed = completed;
				this.used = used;
			}

			static <T> SubscriberStatus<T> create(Optional<T> latest,
					boolean completed, boolean used) {
				return new SubscriberStatus<T>(latest, completed, used);
			}
		}

		private static void addRequest(AtomicLong expected, long n) {
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

		@Override
		public void request(long n) {
			System.out.println(n + " requested");
			if (n <= 0)
				return;

			if (expected.get() == Long.MAX_VALUE)
				return;
			if (n == Long.MAX_VALUE)
				expected.set(n);
			else {
				addRequest(expected, n);
			}

			if (firstTime.compareAndSet(true, false)) {
				boolean first = true;
				for (SourceSubscriber<T> subscriber : subscribers) {
					// don't request the first beccause as soon as we have that
					// we have enough to emit. Wait for a backpressure request
					// first.
					if (first)
						nextRequestFrom.set(0);
					else
						subscriber.requestOneMore();
					first = false;
				}

			}

			performPendingRequest();
		}

		private synchronized void performPendingRequest() {
			System.out.println("draining requests, expected=" + expected
					+ ",nextRequestFrom=" + nextRequestFrom);
			if (expected.get() == 0 || nextRequestFrom.get() == -1)
				return;
			if (expected.get() != Long.MAX_VALUE) {
				expected.decrementAndGet();
			}
			subscribers.get(nextRequestFrom.getAndSet(-1)).requestOneMore();
		}

		private static class IndexValue<T> {
			int index;
			T value;

			IndexValue(int index, T value) {
				this.index = index;
				this.value = value;
			}
		}

		public synchronized void event(int index, Notification<T> event) {
			System.out.println(index + ":" + event);

			if (event.isOnCompleted())
				handleCompleted(index);
			else if (event.isOnError())
				child.onError(event.getThrowable());
			else
				handleOnNext(index, event);
		}

		private void handleOnNext(int index, Notification<T> event) {
			T value = event.getValue();
			SubscriberStatus<T> st = status.get(index);
			status.set(index, SubscriberStatus.create(Optional.of(value),
					st.completed, false));
			process();
		}

		private void handleCompleted(int index) {
			SubscriberStatus<T> st = status.get(index);
			status.set(index, SubscriberStatus.create(st.latest, true, st.used));
			while (process())
				;
			if (countActive() == 0)
				child.onCompleted();
		}

		private boolean process() {
			// if there are enough values then select one for emission and
			// emit it to the child subscriber
			List<IndexValue<T>> indexValues = getIndexValues();
			int active = countActive();
			if (indexValues.size() >= active && indexValues.size() > 0) {
				final IndexValue<T> selected = select(indexValues);
				SubscriberStatus<T> st = status.get(selected.index);
				status.set(selected.index, SubscriberStatus.<T> create(
						of(selected.value), st.completed, true));
				System.out.println("-> " + selected.value);
				child.onNext(selected.value);
				worker.schedule(new Action0() {
					@Override
					public void call() {
						if (!status.get(selected.index).completed) {
							nextRequestFrom.set(selected.index);
							performPendingRequest();
						} else
							process();
					}
				});
				return true;
			} else
				return false;
		}

		private int countNotCompleted() {
			int count = 0;
			for (int i = 0; i < status.length(); i++) {
				if (!status.get(i).completed)
					count++;
			}
			return count;
		}

		private int countActive() {
			int active = 0;
			for (int i = 0; i < status.length(); i++) {
				if (!status.get(i).used || !status.get(i).completed)
					active++;
			}
			return active;
		}

		private List<IndexValue<T>> getIndexValues() {
			List<IndexValue<T>> indexValues = new ArrayList<IndexValue<T>>();
			for (int i = 0; i < status.length(); i++) {
				if (!status.get(i).used && status.get(i).latest.isPresent())
					indexValues.add(new IndexValue<T>(i, status.get(i).latest
							.get()));
			}
			return indexValues;
		}

		private IndexValue<T> select(List<IndexValue<T>> indexValues) {
			List<T> a = new ArrayList<T>(indexValues.size());
			for (IndexValue<T> iv : indexValues) {
				a.add(iv.value);
			}
			return indexValues.get(selector.call(a));
		}

	}

	private static class SourceSubscriber<T> extends Subscriber<T> {

		private final MyProducer<T> producer;
		private final int index;

		SourceSubscriber(MyProducer<T> producer, int index) {
			this.producer = producer;
			this.index = index;
		}

		void requestOneMore() {
			System.out.println("requesting one more from " + index);
			request(1);
		}

		@Override
		public void onStart() {
			// don't request any yet
			request(0);
		}

		@Override
		public void onCompleted() {
			producer.event(index, Notification.<T> createOnCompleted());
		}

		@Override
		public void onError(Throwable e) {
			producer.event(index, Notification.<T> createOnError(e));
		}

		@Override
		public void onNext(T t) {
			producer.event(index, Notification.<T> createOnNext(t));
		}

	}

}
