package com.aliware.tianchi;

import com.aliware.tianchi.exception.ErrorMsg;
import com.aliware.tianchi.util.AllUtil;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

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
    private static final String ACTIVELIMIT_FILTER_START_TIME = "activelimit_filter_start_time";

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        URL url = invoker.getUrl();
        String methodName = invocation.getMethodName();
        RpcContext.getClientAttachment().setAttachment(TIMEOUT_KEY, 15);
        int max = 10; // todo
        final RpcStatus rpcStatus = RpcStatus.getStatus(invoker.getUrl(), invocation.getMethodName());
        if (!RpcStatus.beginCount(url, methodName, max)) {
            throw new RpcException(RpcException.LIMIT_EXCEEDED_EXCEPTION,
                    "=.= Waiting concurrent invoke timeout in client-side for service:  " +
                            invoker.getInterface().getName() + ", method: " + invocation.getMethodName() +
                            ". concurrent invokes: " +
                            rpcStatus.getActive() + ". max concurrent invoke limit: " + max);
        }

        invocation.put(ACTIVELIMIT_FILTER_START_TIME, System.currentTimeMillis());
        System.out.println("active: " + rpcStatus.getActive() + " max: " + max);
        return invoker.invoke(invocation);
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        String methodName = invocation.getMethodName();
        URL url = invoker.getUrl();
        int max = invoker.getUrl().getMethodParameter(methodName, ACTIVES_KEY, 0);

        RpcStatus.endCount(url, methodName, getElapsed(invocation), true);
        System.out.println("+succ: " + RpcStatus.getStatus(url).getSucceeded());
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        System.out.println(t);
        String methodName = invocation.getMethodName();
        URL url = invoker.getUrl();
        int max = invoker.getUrl().getMethodParameter(methodName, ACTIVES_KEY, 0);

        if (t instanceof RpcException) {
            RpcException rpcException = (RpcException) t;
            if (rpcException.isLimitExceed()) {
                return;
            }
        }
        RpcStatus.endCount(url, methodName, getElapsed(invocation), false);
        System.out.println("-fail: " + RpcStatus.getStatus(url).getFailed());
    }

    private long getElapsed(Invocation invocation) {
        Object beginTime = invocation.get(ACTIVELIMIT_FILTER_START_TIME);
        return beginTime != null ? System.currentTimeMillis() - (Long) beginTime : 0;
    }
}