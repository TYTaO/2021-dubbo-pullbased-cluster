package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import oshi.SystemInfo;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.rpc.Constants.EXECUTES_KEY;

/**
 * 服务端过滤器
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.PROVIDER)
public class TestServerFilter implements Filter, BaseFilter.Listener {

    private static final AtomicInteger countToMax = new AtomicInteger(0);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        URL url = invoker.getUrl();
        final MyCount myCount = MyCount.getCount(url);
        int max = myCount.getMax(); // todo
        if (!myCount.beginCount(url, max)) {
            countToMax.incrementAndGet();
            throw new RpcException(RpcException.LIMIT_EXCEEDED_EXCEPTION,
                    "Failed to invoke method " + invocation.getMethodName() + " in provider " +
                            url + ", cause: The service using threads greater than <dubbo:service executes=\"" + max +
                            "\" /> limited.");
        }

        try {
            Result result = invoker.invoke(invocation);
//            System.out.println("active: " + myCount.getActive() + " max: " + max + " maxToGetCount: " + countToMax.get());
            return result;
        } catch (Throwable t) {
            if (t instanceof RuntimeException) {
                throw (RuntimeException) t;
            } else {
                throw new RpcException("unexpected exception when ExecuteLimitFilter", t);
            }
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        URL url = invoker.getUrl();

        MyCount.endCount(url, true);

    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
//        System.out.println("== " + t);
        URL url = invoker.getUrl();

        if (t instanceof RpcException) {
            RpcException rpcException = (RpcException) t;
            if (rpcException.isLimitExceed()) {
                return;
            }
        }
        MyCount.endCount(url, false);
//        System.out.println("-fail: " + MyCount.getCount(url).getFailed());
    }
}
