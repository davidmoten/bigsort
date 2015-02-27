package com.github.davidmoten.bigsort;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.schedulers.Schedulers;

import com.github.davidmoten.rx.Checked;
import com.github.davidmoten.rx.Strings;
import com.github.davidmoten.rx.testing.TestingHelper;
import com.github.davidmoten.util.Optional;

public class BigSortTest extends TestCase {

    private static final Logger log = LoggerFactory.getLogger(BigSortTest.class);

    public static TestSuite suite() {
        return TestingHelper.function(sorter(2, 2, Schedulers.immediate()))
                .waitForTerminalEvent(100, TimeUnit.MILLISECONDS)
                .waitForMoreTerminalEvents(100, TimeUnit.MILLISECONDS)
                // test empty
                .name("testSortOfEmptyReturnsEmpty").fromEmpty().expectEmpty().name("testSortAFew")
                .from(3, 2, 1).expect(1, 2, 3).name("testSortAFew").from(3, 2, 1).expect(1, 2, 3)
                // return suite
                .testSuite(BigSortTest.class);
    }

    public void testSort128by1MaxTemp2() {
        performTest(128, 1, 2);
    }

    public void testSort128by2MaxTemp2() {
        performTest(128, 2, 2);
    }

    public void testSort128by1MaxTemp10() {
        performTest(128, 1, 10);
    }

    public void testSort128by64MaxTemp10() {
        performTest(128, 64, 10);
    }

    public void testSort128by256MaxTemp10() {
        performTest(128, 256, 10);
    }

    public void testSort100Kby10KMaxTemp10() {
        performTest(100000, 10000, 10);
    }

    public void testSortMany() {

    }

    public void testSortUsingSerialization() {
        performSerializableTest(1000, 100, 5);
    }

    private void performSerializableTest(int n, int maxToSortPerThread, int maxTempFiles) {
        Observable<Integer> source = Observable.range(1, n).map(i -> n - i + 1);
        BigSort.sort(source, maxToSortPerThread, maxTempFiles, Schedulers.immediate()).subscribe(
                new Subscriber<Integer>() {
                    Integer last = null;

                    @Override
                    public void onCompleted() {
                        assertEquals((int) last, n);
                    }

                    @Override
                    public void onError(Throwable e) {
                        throw new RuntimeException(e);
                    }

                    @Override
                    public void onNext(Integer i) {
                        if (last != null && i != last + 1) {
                            throw new RuntimeException("not sorted!");
                        }
                        last = i;
                    }
                });

    }

    // public void testSort1Mby100KMaxTemp10() {
    // performTest(1000000, 100000, 10);
    // }

    private static void performTest(int size, int maxToSortPerThread, int maxTempFiles) {
        log.info("sorting " + size + " values using maxToSort=" + maxToSortPerThread
                + " and maxTempFiles=" + maxTempFiles);
        final int n = size;// passes on 127!!
        // source is n, n-1, .., 0
        Observable<Integer> source = createDescendingRange(n);
        List<Integer> range = Observable.range(1, n).toList().toBlocking().single();
        long t = System.currentTimeMillis();
        assertEquals(range,
                sorter(maxToSortPerThread, maxTempFiles, Schedulers.immediate()).call(source)
                        .toList().toBlocking().single());
        log.info("time to sort using immediate() = " + ((System.currentTimeMillis() - t) / 1000.0)
                + "s");
        t = System.currentTimeMillis();
        assertEquals(range, sorter(maxToSortPerThread, maxTempFiles, Schedulers.computation())
                .call(source).toList().toBlocking().single());
        log.info("time to sort using computation() = "
                + ((System.currentTimeMillis() - t) / 1000.0) + "s");
    }

    private static Observable<Integer> createDescendingRange(final int n) {
        return Observable.range(0, n).map(i -> n - i);
    }

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private static Func1<Observable<Integer>, Observable<Integer>> sorter(
            final int maxToSortInMemoryPerThread, final int maxTempResources, Scheduler scheduler) {
        return source -> {
            Comparator<Integer> comparator = createComparator();
            Func2<Observable<Integer>, File, Observable<File>> writer = createWriter();
            Func1<File, Observable<Integer>> reader = createReader();
            Func0<File> resourceFactory = BigSort.createFileResourceFactory(Optional.of("target"));
            Action1<File> resourceDisposer = BigSort.createFileResourceDisposer();

            return BigSort.sort(source, comparator, writer, reader, resourceFactory,
                    resourceDisposer, maxToSortInMemoryPerThread, maxTempResources, scheduler);
        };
    }

    private static Comparator<Integer> createComparator() {
        return (o1, o2) -> o1.compareTo(o2);
    }

    private static Func1<File, Observable<Integer>> createReader() {
        return file -> {
            Observable<String> strings =
            // read the strings from a file
            Strings.from(file);

            return
            // split/join the strings by new line character
            Strings.split(strings, "\n")
            // non-blank lines only
                    .filter(nonEmptyLines())
                    // log
                    // .doOnNext(log())
                    // to an integer
                    .map(toInteger());
        };
    }

    private static Func2<Observable<Integer>, File, Observable<File>> createWriter() {
        return (lines, file) -> {
            // log.info("creating writer for " + file);
            Func0<OutputStream> resourceFactory = Checked.f0(() -> new BufferedOutputStream(
                    new FileOutputStream(file)));

            Func1<OutputStream, Observable<Integer>> observableFactory = os -> lines
                    .doOnNext(Checked.a1(n -> os.write((n + "\n").getBytes(UTF8))));

            Action1<OutputStream> disposeAction = Checked.a1(os -> os.close());

            return Observable.using(resourceFactory, observableFactory, disposeAction, true)
                    .count().map(count -> file);
        };
    }

    private static Func1<String, Boolean> nonEmptyLines() {
        return s -> s.length() > 0;
    }

    private static Func1<String, Integer> toInteger() {
        return s -> Integer.parseInt(s);
    }

}
