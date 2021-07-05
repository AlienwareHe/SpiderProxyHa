package com.virjar.spider.proxy.ha.handlers;

import com.google.common.collect.Lists;
import com.virjar.spider.proxy.ha.core.HaProxyMapping;
import com.virjar.spider.proxy.ha.handlers.upstream.HttpUpstreamHandShaker;
import com.virjar.spider.proxy.ha.handlers.upstream.HttpsUpstreamHandShaker;
import com.virjar.spider.proxy.ha.handlers.upstream.UpstreamHandShaker;
import com.virjar.spider.proxy.ha.utils.HttpNettyUtils;
import com.virjar.spider.proxy.ha.utils.NettyUtils;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpRequestEncoder;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.regex.Pattern;

@Slf4j
public class HttpServerHandler extends ChannelDuplexHandler {
    private ChannelHandlerContext ctx;
    private HttpRequest httpRequest;
    private HaProxyMapping haProxyMapping;
    private boolean isHttps;

    private static final int MAX_INITIAL_LINE_LENGTH_DEFAULT = 8192;
    private static final int MAX_HEADER_SIZE_DEFAULT = 8192 * 2;
    private static final int MAX_CHUNK_SIZE_DEFAULT = 8192 * 2;

    private LinkedList<HttpObject> httpObjects = Lists.newLinkedList();

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        this.ctx = ctx;

        if (msg instanceof HttpRequest) {
            httpRequest = (HttpRequest) msg;
            httpObjects.addLast(httpRequest);
        } else if (msg instanceof HttpObject) {
            httpObjects.addLast((HttpObject) msg);
            return;
        } else {
            log.warn("can not handle message: {}", msg);
            ReferenceCountUtil.release(msg);
            ctx.close();
            return;
        }

        log.info("Received raw request from ip:{} local:{}: request:{} ", ctx.channel().remoteAddress(), ctx.channel().localAddress(), httpRequest);

        if (httpRequest.getDecoderResult().isFailure()) {
            log.warn("Could not parse request from client. Decoder result: {}", httpRequest.getDecoderResult().toString());
            FullHttpResponse response = HttpNettyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.BAD_REQUEST,
                    "Unable to parse HTTP request");
            HttpHeaders.setKeepAlive(response, false);
            HttpNettyUtils.respondWithShortCircuitResponse(ctx.channel(), response);
            ctx.close();
            return;
        }

        isHttps = HttpNettyUtils.isCONNECT(httpRequest);
        if (!isHttps) {
            // http代理模式，需要暂停读，否则客户端可能一直发数据过来
            ctx.channel().config().setAutoRead(false);
        }


        if (isRequestToOriginServer()) {
            // 不是代理请求，而是直连到代理服务中
            // 此时认为他是想操作服务器
            // 比如feedback代理质量、发送指令切换上游代理ip映射、获取当前代理绑定上游ip、获取当前代理的真实出口ip等
            handleApiHttpRequest();
            return;
        }

        // 创建到 后端代理资源的链接
        haProxyMapping = HaProxyMapping.get(ctx.channel());

        haProxyMapping.borrowConnect(
                value -> {
                    if (value == null) {
                        log.warn("connect to upstream proxy server failed:{} ", haProxyMapping.resourceKey());
                        HttpNettyUtils.writeBadRequest(ctx.channel(), httpRequest);
                        return;
                    }
                    onUpstreamConnectionEstablish(value);
                }
        );
    }

    private void onUpstreamConnectionEstablish(Channel upstreamChannel) {
        NettyUtils.makePair(ctx.channel(), upstreamChannel);
        NettyUtils.loveOther(ctx.channel(), upstreamChannel);

        UpstreamHandShaker.UpstreamHandSharkCallback callback = new UpstreamHandShaker.UpstreamHandSharkCallback() {
            @Override
            public void onHandSharkFailed(String message) {
                HttpNettyUtils.writeBadRequest(ctx.channel(), httpRequest);
            }

            @Override
            public void onHandSharkSuccess() {
                if (isHttps) {
                    onHttpsHandSharkFinish(upstreamChannel);
                } else {
                    ctx.channel().config().setAutoRead(true);
                    onHttpHandSharkFinish(upstreamChannel);
                }
            }
        };

        UpstreamHandShaker<HttpRequest> handShaker;
        if (isHttps) {
            handShaker = new HttpsUpstreamHandShaker(upstreamChannel, haProxyMapping.getSource(), callback, httpRequest);
        } else {
            handShaker = new HttpUpstreamHandShaker(upstreamChannel, haProxyMapping.getSource(), callback, httpRequest);
        }
        handShaker.doHandShark();
    }

    private void onHttpsHandSharkFinish(Channel upstreamChannel) {
        HttpResponse response = HttpNettyUtils.createFullHttpResponse(HttpVersion.HTTP_1_1,
                CONNECTION_ESTABLISHED);
        response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        HttpNettyUtils.addVia(response, "virjar-spider-ha-proxy");
        ctx.channel().writeAndFlush(response)
                .addListener((ChannelFutureListener) channelFuture -> {
                    if (!channelFuture.isSuccess()) {
                        //TODO 未来这里可以复用？？
                        upstreamChannel.close();
                        return;
                    }
                    ChannelPipeline pipeline = ctx.pipeline();
                    pipeline.remove(HttpResponseEncoder.class);
                    pipeline.remove(HttpRequestDecoder.class);
                    pipeline.remove(HttpServerHandler.class);

                    pipeline.addLast(new RelayHandler(upstreamChannel));
                    upstreamChannel.pipeline().addLast(new RelayHandler(channelFuture.channel()));
                });
    }

    private void appendHttpContent(Channel outboundChannel) {
        EventExecutor executor = ctx.executor();
        if (!executor.inEventLoop()) {
            executor.execute(() -> appendHttpContent(outboundChannel));
            return;
        }
        HttpObject httpObject = httpObjects.pollFirst();
        if (httpObject == null) {
            return;
        }

        if (!outboundChannel.isActive()) {
            log.warn("outbound socket has been closed");
            ctx.close();
            return;
        }
        outboundChannel.writeAndFlush(httpObject).addListener(future -> {
            if (!future.isSuccess()) {
                ctx.close();
                log.warn("outbound socket has been closed when append http object");
                return;
            }

            if (!httpObjects.isEmpty()) {
                appendHttpContent(outboundChannel);
                return;
            }
            log.info("http message write finish ,start http tuning");
            ctx.channel().pipeline().addLast(new RelayHandler(outboundChannel));
            outboundChannel.pipeline().addLast(new RelayHandler(ctx.channel()));
            ctx.pipeline().remove(HttpServerHandler.class);
            ctx.channel().config().setAutoRead(true);
        });
    }

    private void onHttpHandSharkFinish(Channel upstreamChannel) {
        ChannelPipeline pipeline = upstreamChannel.pipeline();
        pipeline.addLast(new HttpRequestEncoder());
        pipeline.addLast(new HttpResponseDecoder(MAX_INITIAL_LINE_LENGTH_DEFAULT,
                MAX_HEADER_SIZE_DEFAULT,
                MAX_CHUNK_SIZE_DEFAULT));

        appendHttpContent(upstreamChannel);
    }


    private void handleApiHttpRequest() {
        // 暂时都返回502，后续再处理真实业务逻辑
        HttpNettyUtils.writeBadRequest(ctx.channel(), httpRequest);
    }


    private boolean isRequestToOriginServer() {
        if (httpRequest.getMethod() == HttpMethod.CONNECT) {
            return false;
        }

        // direct requests to the proxy have the path only without a scheme
        String uri = httpRequest.getUri();
        return !HTTP_SCHEME.matcher(uri).matches();
    }

    private static final Pattern HTTP_SCHEME = Pattern.compile("^http://.*", Pattern.CASE_INSENSITIVE);

    private static final HttpResponseStatus CONNECTION_ESTABLISHED = new HttpResponseStatus(
            200, "Connection established");
}
