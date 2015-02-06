package com.github.davidmoten.bigsort;

import java.util.LinkedList;
import java.util.List;

import rx.Observable;
import rx.Observable.Operator;
import rx.Subscriber;

public class OperatorResourceWriter<Resource> implements
		Operator<Resource, Resource> {

	public OperatorResourceWriter(List<Observable<Resource>> sources) {

	}

	@Override
	public Subscriber<? super Resource> call(Subscriber<? super Resource> child) {
		final List<Resource> resources = new LinkedList<Resource>();
		return new Subscriber<Resource>(child) {

			@Override
			public void onCompleted() {
				// TODO Auto-generated method stub

			}

			@Override
			public void onError(Throwable e) {
				// TODO Auto-generated method stub

			}

			@Override
			public void onNext(Resource t) {
				// TODO Auto-generated method stub

			}

		};
	}
}
