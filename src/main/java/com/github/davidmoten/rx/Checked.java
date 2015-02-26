package com.github.davidmoten.rx;

import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;

//TODO use from rxjava-extras once on Maven Central
public final class Checked {

    public static interface F0<T> {
        T call() throws Exception;
    }
    public static interface F1<T,R> {
        R call(T t) throws Exception;
    }
    
    public static interface F2<T,R,S> {
        S call(T t,R r) throws Exception;
    }
    
    
    public static interface A0 {
        void call() throws Exception;
    }
    
    public static interface A1<T> {
        void call(T t) throws Exception;
    }
    
    public static <T> Func0<T> f0(final F0<T> f) {
        return new Func0<T>(){

            @Override
            public T call() {
                try {
                    return f.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            
        };
    }
    
    public static <T,R> Func1<T,R> f1(final F1<T,R> f) {
        return new Func1<T,R>(){

            @Override
            public R call(T t) {
                try {
                    return f.call(t);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            
        };
    }
    
    public static <T,R,S> Func2<T,R,S> f2(final F2<T,R,S> f) {
        return new Func2<T,R,S>(){

            @Override
            public S call(T t,R r) {
                try {
                    return f.call(t,r);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            
        };
    }
    
    public static Action0 a0(final A0 a) {
        return new Action0(){

            @Override
            public void call() {
                try {
                    a.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
    
    public static <T> Action1<T> a1(final A1<T> a) {
        return new Action1<T>(){

            @Override
            public void call(T t) {
                try {
                    a.call(t);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
    
    
}
