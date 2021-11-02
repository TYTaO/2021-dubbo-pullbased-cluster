package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.*;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 服务端过滤器
 * 可选接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 用户可以在服务端拦截请求和响应,捕获 rpc 调用时产生、服务端返回的已知异常。
 */
@Activate(group = CommonConstants.PROVIDER)
public class TestServerFilter implements Filter, BaseFilter.Listener {

    private static final String MAX_CONCURRENT = "max_concurrent";
    private static final String ACTIVES = "ACTIVES";
    private static final String FINE_TUNE_FROM_CLIENT = "fineTuneFromClient"; // 正式阶段消费端对服务端的微调


    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        URL url = invoker.getUrl();
        final ServerRpcStatus serverRpcStatus = ServerRpcStatus.getStatus(url);
        int max = serverRpcStatus.getConcurrentLimit(); // todo
        Integer fineTuneFromClient = Integer.valueOf(invocation.getAttachment(FINE_TUNE_FROM_CLIENT));
        if (fineTuneFromClient != 0) {
            serverRpcStatus.bestConcurrentLimit.set(max + fineTuneFromClient);
            System.out.println("max: " + max + fineTuneFromClient);
        }
        if (!serverRpcStatus.beginCount(url, max)) {
            throw new RpcException(RpcException.LIMIT_EXCEEDED_EXCEPTION,
                    "Failed to invoke method " + invocation.getMethodName() + " in provider " +
                            url + ", cause: The service using threads greater than <dubbo:service executes=\"" + max +
                            "\" /> limited.");
        }

        try {
            invocation.put(ACTIVES, serverRpcStatus.getActive());
            Result result = invoker.invoke(invocation);
//            System.out.println("active: " + serverRpcStatus.getActive() + " max: " + max + " maxToGetCount: " + countToMax.get());
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
        int maxConcurrent = ServerRpcStatus.getStatus(url).getConcurrentLimitToClient();

        // 给客户端返回最大并发限制以及当前并发
        appResponse.setAttachment(MAX_CONCURRENT, String.valueOf(maxConcurrent));
        appResponse.setAttachment(ACTIVES, String.valueOf(getActives(invocation)));

        ServerRpcStatus.endCount(url, true);
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {
        URL url = invoker.getUrl();

        if (t instanceof RpcException) {
            RpcException rpcException = (RpcException) t;
            if (rpcException.isLimitExceed()) {
                return;
            }
        }
        ServerRpcStatus.endCount(url, false);
    }

    private long getActives(Invocation invocation) {
        Object actives = invocation.get(ACTIVES);
        return (int) actives;
    }
}
