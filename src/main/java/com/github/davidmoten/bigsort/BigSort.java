package com.github.davidmoten.bigsort;

import java.io.File;

import rx.Observable;
import rx.functions.Action2;
import rx.functions.Func1;

public class BigSort {

	public static interface Writer<T> extends Action2<Observable<T>, File> {

	}

	public static interface Reader<T> {
		Observable<T> read(File file);
	}

	public static <T> Observable<T> sort(Observable<T> source,
			Action2<Observable<T>, File> writer,
			Func1<File, Observable<T>> reader) {
		return null;
	}

}
