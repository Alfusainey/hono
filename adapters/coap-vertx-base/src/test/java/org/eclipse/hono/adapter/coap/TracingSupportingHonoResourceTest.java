/**
 * Copyright (c) 2020 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 */


package org.eclipse.hono.adapter.coap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.Executor;

import org.eclipse.californium.core.coap.CoAP.Code;
import org.eclipse.californium.core.coap.CoAP.ResponseCode;
import org.eclipse.californium.core.coap.Option;
import org.eclipse.californium.core.coap.Request;
import org.eclipse.californium.core.network.Exchange;
import org.eclipse.californium.core.network.Exchange.Origin;
import org.eclipse.californium.core.server.resources.CoapExchange;
import org.eclipse.hono.auth.Device;
import org.eclipse.hono.util.TenantObject;
import org.eclipse.hono.util.TenantObjectWithAuthId;
import org.eclipse.hono.util.TenantTracingConfig;
import org.eclipse.hono.util.TracingSamplingMode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.opentracing.References;
import io.opentracing.Span;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.propagation.Binary;
import io.opentracing.propagation.Format;
import io.opentracing.tag.Tags;
import io.vertx.core.Future;

/**
 * Tests verifying behavior of {@code TracingSupportingHonoResource}.
 *
 */
public class TracingSupportingHonoResourceTest {

    private static final String TENANT_ID = "tenant";
    private static final String DEVICE_ID = "4711";
    private static final String AUTHENTICATED_DEVICE_ID = "4712";
    private static final String AUTH_ID = "authId";

    private Tracer tracer;
    private Span span;
    private SpanBuilder spanBuilder;
    private TracingSupportingHonoResource resource;
    private CoapContextTenantAndAuthIdProvider tenantObjectWithAuthIdProvider;

    /**
     * Sets up the fixture.
     */
    @BeforeEach
    public void setUp() {
        span = mock(Span.class);
        spanBuilder = mock(SpanBuilder.class, RETURNS_SELF);
        when(spanBuilder.start()).thenReturn(span);
        tracer = mock(Tracer.class);
        when(tracer.buildSpan(anyString())).thenReturn(spanBuilder);

        tenantObjectWithAuthIdProvider = mock(CoapContextTenantAndAuthIdProvider.class);
        final TenantObject tenantObject = TenantObject.from(TENANT_ID, true);
        final TenantObjectWithAuthId tenantObjectWithAuthId = new TenantObjectWithAuthId(tenantObject, AUTH_ID);

        when(tenantObjectWithAuthIdProvider.get(any(CoapContext.class), any()))
                .thenReturn(Future.succeededFuture(tenantObjectWithAuthId));
        resource = new TracingSupportingHonoResource(tracer, "test", "adapter", tenantObjectWithAuthIdProvider) {

            @Override
            protected Future<CoapContext> createCoapContextForPost(final CoapExchange coapExchange) {
                return Future.succeededFuture(createCoapContext(coapExchange));
            }

            @Override
            protected Future<CoapContext> createCoapContextForPut(final CoapExchange coapExchange) {
                return Future.succeededFuture(createCoapContext(coapExchange));
            }

            @Override protected Future<ResponseCode> handlePost(final CoapContext coapContext) {
                return Future.succeededFuture(ResponseCode.CHANGED);
            }
        };
    }

    private CoapContext createCoapContext(final CoapExchange coapExchange) {
        return CoapContext.fromRequest(coapExchange, new Device(TENANT_ID, DEVICE_ID),
                new Device(TENANT_ID, AUTHENTICATED_DEVICE_ID), AUTH_ID);
    }

    /**
     * Verifies that the resource uses the SpanContext extracted from a CoAP request
     * as the parent of the newly created Span.
     */
    @Test
    public void testHandleRequestExtractsParentTraceContext() {

        final SpanContext extractedContext = mock(SpanContext.class);
        when(tracer.extract(eq(Format.Builtin.BINARY), any(Binary.class))).thenReturn(extractedContext);

        final Request request = new Request(Code.POST);
        request.getOptions().addOption(new Option(CoapOptionInjectExtractAdapter.OPTION_TRACE_CONTEXT));
        final Exchange exchange = new Exchange(request, Origin.REMOTE, mock(Executor.class));
        resource.handleRequest(exchange);

        verify(tracer).buildSpan(eq(Code.POST.toString()));
        verify(spanBuilder).withTag(eq(Tags.SPAN_KIND.getKey()), eq(Tags.SPAN_KIND_SERVER.toString()));
        verify(spanBuilder).addReference(eq(References.CHILD_OF), eq(extractedContext));
    }

    /**
     * Verifies that the resource does not set a parent on the newly created Span if the CoAP request
     * does not contain a trace context option.
     */
    @Test
    public void testExtractFromEmptyOptionSet() {

        final Request request = new Request(Code.POST);
        final Exchange exchange = new Exchange(request, Origin.REMOTE, mock(Executor.class));
        resource.handleRequest(exchange);

        verify(tracer, never()).extract(eq(Format.Builtin.BINARY), any(Binary.class));
        verify(tracer).buildSpan(eq(Code.POST.toString()));
        verify(spanBuilder).withTag(eq(Tags.SPAN_KIND.getKey()), eq(Tags.SPAN_KIND_SERVER.toString()));
        verify(spanBuilder).addReference(eq(References.CHILD_OF), isNull());
    }

    /**
     * Verifies that the resource sets the trace sampling priority on the newly created Span if the CoAP request
     * belongs to a tenant for which a specific sampling priority is configured.
     */
    @Test
    public void testApplyTenantTraceSamplingPriority() {

        final TenantObject tenantObject = TenantObject.from(TENANT_ID, true);
        final TenantTracingConfig tracingConfig = new TenantTracingConfig();
        tracingConfig.setSamplingMode(TracingSamplingMode.NONE);
        tenantObject.setTracingConfig(tracingConfig);
        final TenantObjectWithAuthId tenantObjectWithAuthId = new TenantObjectWithAuthId(tenantObject, AUTH_ID);

        when(tenantObjectWithAuthIdProvider.get(any(CoapContext.class), any()))
                .thenReturn(Future.succeededFuture(tenantObjectWithAuthId));

        final Request request = new Request(Code.POST);
        final Exchange exchange = new Exchange(request, Origin.REMOTE, mock(Executor.class));
        resource.handleRequest(exchange);

        verify(tracer).buildSpan(eq(Code.POST.toString()));
        verify(spanBuilder).withTag(eq(Tags.SPAN_KIND.getKey()), eq(Tags.SPAN_KIND_SERVER.toString()));
        verify(spanBuilder).addReference(eq(References.CHILD_OF), isNull());
        // verify sampling prio has been set to 0 (corresponding to TracingSamplingMode.NONE)
        verify(span).setTag(eq(Tags.SAMPLING_PRIORITY.getKey()), eq(0));
        verify(span).setBaggageItem(eq(Tags.SAMPLING_PRIORITY.getKey()), eq("0"));
    }

    /**
     * Verifies that the resource sets the trace sampling priority on the newly created Span if the CoAP request
     * belongs to a tenant and an auth-id for which a specific sampling priority is configured.
     */
    @Test
    public void testApplyTenantTraceSamplingPrioritySetForAuthId() {

        final TenantObject tenantObject = TenantObject.from(TENANT_ID, true);
        final TenantTracingConfig tracingConfig = new TenantTracingConfig();
        tracingConfig.setSamplingModePerAuthId(Map.of(AUTH_ID, TracingSamplingMode.ALL));
        tenantObject.setTracingConfig(tracingConfig);
        final TenantObjectWithAuthId tenantObjectWithAuthId = new TenantObjectWithAuthId(tenantObject, AUTH_ID);

        when(tenantObjectWithAuthIdProvider.get(any(CoapContext.class), any()))
                .thenReturn(Future.succeededFuture(tenantObjectWithAuthId));

        final Request request = new Request(Code.POST);
        final Exchange exchange = new Exchange(request, Origin.REMOTE, mock(Executor.class));
        resource.handleRequest(exchange);

        verify(tracer).buildSpan(eq(Code.POST.toString()));
        verify(spanBuilder).withTag(eq(Tags.SPAN_KIND.getKey()), eq(Tags.SPAN_KIND_SERVER.toString()));
        verify(spanBuilder).addReference(eq(References.CHILD_OF), isNull());
        // verify sampling prio has been set to 1 (corresponding to TracingSamplingMode.ALL)
        verify(span).setTag(eq(Tags.SAMPLING_PRIORITY.getKey()), eq(1));
        verify(span).setBaggageItem(eq(Tags.SAMPLING_PRIORITY.getKey()), eq("1"));
    }
}
