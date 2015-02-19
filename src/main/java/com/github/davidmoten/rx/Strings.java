package com.github.davidmoten.rx;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;

import com.github.davidmoten.rx.operators.OnSubscribeUsing2;
import com.github.davidmoten.rx.operators.ReaderOnSubscribe;
import com.github.davidmoten.rx.operators.StringSplitOperator;

/**
 * Utilities for stream processing of lines of text from
 * <ul>
 * <li>sockets</li>
 * </ul>
 */
public final class Strings {

	private static final Logger log = LoggerFactory.getLogger(Strings.class);

	public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

	/**
	 * Returns null if input is null otherwise returns input.toString().trim().
	 */
	public static Func1<Object, String> TRIM = new Func1<Object, String>() {

		@Override
		public String call(Object input) {
			if (input == null)
				return null;
			else
				return input.toString().trim();
		}
	};

	public static Observable<String> from(final Reader reader, final int size) {
		return new ReaderOnSubscribe(reader, size).toObservable();
		// return StringObservable.from(reader, size);
	}

	public static Observable<String> from(Reader reader) {
		return from(reader, 8192);
	}

	public static Observable<String> split(Observable<String> source,
			String pattern) {
		return source.lift(new StringSplitOperator(Pattern.compile(pattern)));
	}

	public static Observable<String> from(File file) {
		return from(file, DEFAULT_CHARSET);
	}

	public static Observable<String> from(final File file, final Charset charset) {
		Func0<Reader> resourceFactory = new Func0<Reader>() {
			@Override
			public Reader call() {
				try {
					log.info("opening reader for " + file);
					return new InputStreamReader(new FileInputStream(file),
							charset);
				} catch (FileNotFoundException e) {
					throw new RuntimeException(e);
				}
			}
		};
		Func1<Reader, Observable<String>> observableFactory = new Func1<Reader, Observable<String>>() {
			@Override
			public Observable<String> call(Reader is) {
				return from(is);
			}
		};
		Action1<Reader> disposeAction = new Action1<Reader>() {
			@Override
			public void call(Reader is) {
				try {
					log.info("closing reader for " + file);
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		return Observable.create(new OnSubscribeUsing2<String, Reader>(
				resourceFactory, observableFactory, disposeAction, true));
	}

}
