/**
 * Copyright 2014 Netflix, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.davidmoten.rx.operators;

import java.util.concurrent.atomic.AtomicBoolean;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.subscriptions.Subscriptions;

/**
 * Constructs an observable sequence that depends on a resource object.
 */
public final class OnSubscribeUsingDisposeBeforeComplete<T, Resource>
		implements OnSubscribe<T> {

	private final Func0<Resource> resourceFactory;
	private final Func1<? super Resource, ? extends Observable<? extends T>> observableFactory;
	private final Action1<? super Resource> dispose;

	public OnSubscribeUsingDisposeBeforeComplete(
			Func0<Resource> resourceFactory,
			Func1<? super Resource, ? extends Observable<? extends T>> observableFactory,
			Action1<? super Resource> dispose) {
		this.resourceFactory = resourceFactory;
		this.observableFactory = observableFactory;
		this.dispose = dispose;
	}

	public static <T, Resource> Observable<T> create(
			Func0<Resource> resourceFactory,
			Func1<? super Resource, ? extends Observable<? extends T>> observableFactory,
			Action1<? super Resource> dispose) {
		return Observable.create(new OnSubscribeUsing2<T, Resource>(
				resourceFactory, observableFactory, dispose, true));
	}

	@Override
	public void call(Subscriber<? super T> subscriber) {
		try {
			final Resource resource = resourceFactory.call();
			Action0 disposeAction = new Action0() {

				private final AtomicBoolean disposed = new AtomicBoolean(false);

				@Override
				public void call() {
					if (disposed.compareAndSet(false, true))
						dispose.call(resource);
				}

			};
			subscriber.add(Subscriptions.create(disposeAction));
			Observable<? extends T> observable = observableFactory
					.call(resource);
			observable.doOnCompleted(disposeAction).unsafeSubscribe(subscriber);
		} catch (Throwable e) {
			// eagerly call unsubscribe since this operator is specifically
			// about resource management
			subscriber.unsubscribe();
			// then propagate error
			subscriber.onError(e);
		}
	}

}
