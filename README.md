# bigsort
<a href="https://travis-ci.org/davidmoten/bigsort"><img src="https://travis-ci.org/davidmoten/bigsort.svg"/></a><br/>
Sorts an arbitrarily large stream by serializing sorted chunks to temporary resources (user-definable) and merging those resources.

Status: *pre-alpha*

## Features
* uses [RxJava](https://github.com/ReactiveX/RxJava)
* Configurable concurrency

How to sort lines of text
-------------------------------
Simple string lines sorting example:

```java
import rx.Observable;
import rx.Scheduler;
import com.github.davidmoten.bigsort.BigSort;

Observable<String> source = Observable.just("c", "b", "a");

int maxToSortInMemoryPerThread = 3;
int maxTempFiles = 2;

// sort in parallel
Scheduler scheduler = Schedulers.computation();

Observable<String> sorted = 
        BigSort.sort(source, maxToSortInMemoryPerThread,
                     maxTempFiles, scheduler);
```

How to sort Serializable & Comparable objects
---------------------------------

```java
int n = 1000;
//make a descending list of integers
Observable<Integer> source = Observable.range(1,n).map(i -> n - i + 1);
int maxToSortInMemoryPerThread = 100;
int maxTempFiles = 5;
Scheduler scheduler = Schedulers.computation();
Observable<Integer> sorted = 
        BigSort.sort(source, maxToSortInMemoryPerThread,
                             maxTempFiles, scheduler); 
```

More generic example
-------------------------

A more generic example that sorts a stream of ```Integer```, also using files but some other resource type 
(for example database tables or S3) could be used provided *writer*, *reader*, *resourceFactory*,
*resourceDisposer* are supplied for that resource type.

Below is an extract from [BigSortTest.java](src/test/java/com/github/davidmoten/bigsort/BigSortTest.java):

```java
Comparator<Integer> comparator = createComparator();
Func2<Observable<Integer>, File, Observable<File>> writer = createWriter();
Func1<File, Observable<Integer>> reader = createReader();
Func0<File> resourceFactory = BigSort.createFileResourceFactory();
Action1<File> resourceDisposer = BigSort.createFileResourceDisposer();
// sort synchronously
Scheduler scheduler = Schedulers.immediate();

Observable<Integer> sorted = 
        BigSort.sort(source, comparator, writer, reader,
					resourceFactory, resourceDisposer, 
					maxToSortInMemoryPerThread, maxTempResources,
					scheduler);
```

Benchmarks
---------------
Time to sort a list of integers using ```ObjectOutputStream``` and ```ObjectInputStream``` for serialization (probably pretty slow!) is below. Max number to sort in memory per thread was 100K and max temporary files was 10 using an SSD and 6 cores on a Xeon ES-1650.

| Number of integers | Time (s) |
|--------------|----------|
| 10<sup>9</sup> | 413   |
| 10<sup>8</sup> | 19.4  |
| 10<sup>7</sup> | 0.7   |
| 10<sup>6</sup> | 0.27  |




