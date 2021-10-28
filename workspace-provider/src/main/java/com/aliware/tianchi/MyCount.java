package com.aliware.tianchi;

import org.apache.dubbo.common.URL;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MyCount {

    private static final ConcurrentMap<String, MyCount> SERVICE_STATISTICS = new ConcurrentHashMap<String,
            MyCount>();

    private static final int initMax = 30; // todo change account to local (2) or server (30)
//    private static final int initStep = 3;
//    private static final int maxStepAbs = 3;
//    private static final int minStepAbs = 3;
    private static final long interval = 2500; // 2s
    private static final long preheatInterval = 600; // 2s
//    private static final int stepAbsFix = 5; //
    private static final long preheatIntervalSum = 50000; //50000
    private static final long preheatSumDdl = System.currentTimeMillis() + preheatIntervalSum; //

    private static final int largeChange = 4; // todo change account to local (3) or server (10)
    private static final int littleChange = 4; // todo change account to local (1) or server (3)


    private final AtomicInteger active = new AtomicInteger();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicInteger failed = new AtomicInteger();

//    private final AtomicInteger step = new AtomicInteger(initStep);
//    private final AtomicInteger lastStep = new AtomicInteger(initStep);
    private final AtomicInteger lastReq = new AtomicInteger(0);
    private final AtomicInteger bestReq = new AtomicInteger(0);
    private final AtomicInteger bestMax = new AtomicInteger(0);
    private final AtomicInteger lastMax = new AtomicInteger(0);
    private final AtomicBoolean isFastStart = new AtomicBoolean(true);



    private final AtomicInteger thisReqTmp = new AtomicInteger(0);
    private final AtomicInteger thisMax = new AtomicInteger(initMax);

    private final AtomicLong lastUpdateTime = new AtomicLong(System.currentTimeMillis());


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

    private boolean isOk() {
        return System.currentTimeMillis() > preheatSumDdl;
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
            if (!count.isOk()) {
                count.updateThisReq();
                count.timedUpdateMax();
            }
        } else {
            count.failed.incrementAndGet();
        }
    }

    private void updateThisReq() {
        if (System.currentTimeMillis() > lastUpdateTime.get() + preheatInterval) {
            thisReqTmp.incrementAndGet();
        }
    }

    private void timedUpdateMax() {
        if (System.currentTimeMillis() > lastUpdateTime.get() + interval) {
            synchronized (this) {
                if (System.currentTimeMillis() > lastUpdateTime.get() + interval) {
                    lastUpdateTime.set(System.currentTimeMillis()); // 参考单例模式
                    if (thisReqTmp.get() < 100) { // 待机
                        thisReqTmp.set(0); // 归零
                        return;
                    }
                    int thisReq = thisReqTmp.get();
                    int nextMax = getNextMax(lastReq.get(), thisReq, lastMax.get(), thisMax.get());

                    // update
                    updateBestMax(thisMax.get(), thisReq);
                    System.out.println("max: " + thisMax.get() + " req: " + thisReq + " bestMax: " + bestMax + " bestReq: " + bestReq);

                    lastReq.set(thisReq);
                    lastMax.set(thisMax.get());

                    if (nextMax < 1) { // 极端
                        nextMax = 2;
                        thisMax.set(nextMax);
                    } else {
                        thisMax.set(nextMax);
                    }

                    thisReqTmp.set(0); // 归零
                }
            }
        }
    }

    private void updateBestMax(int thisMax, int thisReq) {
        if (thisReq > bestReq.get()) {
            bestReq.set(thisReq);
            bestMax.set(thisMax);
        }
    }

//    private int getStep(int lastReq, int thisReq, int oldStep) {
//        int slope = (thisReq - lastReq) / oldStep;
//
//        return slope >= 0 ? stepAbsFix : -stepAbsFix;
//    }

    private int getNextMax(int lastReq, int thisReq, int lastMax, int thisMax) {
        boolean isAdd = (thisReq - lastReq) * (thisMax - lastMax) > 0; //

        if (isAdd) {
            if (isFastStart.get()) {
                return thisMax + largeChange;
            } else { // 微调
                return thisMax + littleChange;
            }
        } else {
            if (isFastStart.get()) {
                isFastStart.set(false);
                return thisMax - largeChange;
            } else {
                return thisMax - littleChange;
            }
        }
    }

    public int getMax() {
        if (isOk()) {
            System.out.println("ok" + " best: " + bestMax.get());
            return this.bestMax.get();
        }
        return this.thisMax.get();
    }

    public int getMaxToClient() {
        if(isOk()) {
            return this.bestMax.get();
        }
        return 0;
    }
}
