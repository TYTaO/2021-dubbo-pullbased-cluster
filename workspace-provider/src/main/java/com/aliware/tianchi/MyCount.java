package com.aliware.tianchi;

import org.apache.dubbo.common.URL;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MyCount {

    private static final ConcurrentMap<String, MyCount> SERVICE_STATISTICS = new ConcurrentHashMap<String,
            MyCount>();

    private final AtomicInteger active = new AtomicInteger();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicInteger failed = new AtomicInteger();

    private MyCount() {
    }

    public static MyCount getCount(URL url) {
        String uri = url.toIdentityString();
        return SERVICE_STATISTICS.computeIfAbsent(uri, key -> new MyCount());
    }

    public int getActive() {
        return active.get();
    }

    public long getSucceeded() {
        return succeeded.longValue();
    }

    public long getFailed() {
        return failed.longValue();
    }

    public static boolean beginCount(URL url, int max) {
        max = (max <= 0) ? Integer.MAX_VALUE : max;
        MyCount count = getCount(url);
        if (count.active.get() == Integer.MAX_VALUE) {
            return false;
        }
        for (int i; ; ) {
            i = count.active.get();
            if (i == Integer.MAX_VALUE || i + 1 > max) {
                return false;
            }
            if (count.active.compareAndSet(i, i + 1)) {
                break;
            }
        }
        return true;
    }

    public static void endCount(URL url, boolean succeeded) {
        MyCount count = getCount(url);
        count.active.decrementAndGet();

        if (succeeded) {
            count.succeeded.incrementAndGet();
        } else {
            count.failed.incrementAndGet();
        }
    }
}
