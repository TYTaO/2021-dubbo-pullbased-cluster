package com.aliware.tianchi;

import com.aliware.tianchi.exception.ErrorMsg;
import com.aliware.tianchi.util.AllUtil;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;
import static org.apache.dubbo.rpc.Constants.ACTIVES_KEY;

/**
 * 客户端过滤器（选址后）
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在客户端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.CONSUMER)
public class TestClientFilter implements Filter, BaseFilter.Listener {

    private static final AtomicInteger countToMax = new AtomicInteger(0);

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        URL url = invoker.getUrl();
        int timeout = 5;
        RpcContext.getClientAttachment().setAttachment(TIMEOUT_KEY, timeout);
        int max = 100; // todo
        final MyCount myCount = MyCount.getCount(url);
        if (!myCount.beginCount(url, max)) {
            countToMax.incrementAndGet();
            throw new RpcException(RpcException.LIMIT_EXCEEDED_EXCEPTION,
                    "=.= get to limit concurrent invoke for service:  " +
                            invoker.getInterface().getName() + ", method: " + invocation.getMethodName() +
                            ". concurrent invokes: " +
                            myCount.getActive() + ". max concurrent invoke limit: " + max);
        }

        Result result = invoker.invoke(invocation);
        System.out.println("active: " + myCount.getActive() + " max: " + max + " maxToGetCount: " + countToMax.get());
        return result;
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        URL url = invoker.getUrl();

        MyCount.endCount(url, true);
        System.out.println("+succ: " + MyCount.getCount(url).getSucceeded());
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        System.out.println("== " + t);
        URL url = invoker.getUrl();

        if (t instanceof RpcException) {
            RpcException rpcException = (RpcException) t;
            if (rpcException.isLimitExceed()) {
                return;
            }
        }
        MyCount.endCount(url, false);
        System.out.println("-fail: " + MyCount.getCount(url).getFailed());
    }
}
