/*******************************************************************************
 * Copyright (c) 2018 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 *******************************************************************************/

package org.eclipse.hono.tests.amqp;

import java.net.HttpURLConnection;
import java.util.function.Supplier;

import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.client.MessageConsumer;
import org.eclipse.hono.tests.IntegrationTestSupport;
import org.eclipse.hono.util.CommandConstants;
import org.eclipse.hono.util.EventConstants;
import org.eclipse.hono.util.ResourceIdentifier;
import org.eclipse.hono.util.TenantObject;
import org.eclipse.hono.util.TimeUntilDisconnectNotification;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonMessageHandler;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;

/**
 * Integration tests for sending commands to device connected to the AMQP adapter.
 * 
 */
@RunWith(VertxUnitRunner.class)
public class CommandAndControlAmqpIT extends AmqpAdapterTestBase {

    private static final String TEMPLATE_COMMAND_SUBSCRIBE_ADDRESS = CommandConstants.COMMAND_ENDPOINT + "/%s/%s/req";
    private static final String TEMPLATE_COMMAND_RESPONSE_ADDRESS = "control////%s/%d";

    private String tenantId;
    private String deviceId;
    private String password = "secret";
    private TenantObject tenant;

    /**
     * Sets up the fixture for this test suite.
     */
    @Before
    public void setUp() {
        log.info("running {}", testName.getMethodName());
        tenantId = helper.getRandomTenantId();
        deviceId = helper.getRandomDeviceId(tenantId);
        tenant = TenantObject.from(tenantId, true);
    }

    /**
     * Verifies that the adapter forwards commands and response to respectively fro between
     * an application and a device.
     * 
     * @param ctx Ther Vert.x test context.
     */
    @Test
    @Ignore
    public void testSendCommandSucceeds(final TestContext ctx) {
        final Async setup = ctx.async();
        final Async notificationReceived = ctx.async();

        final String sourceAddress = String.format(TEMPLATE_COMMAND_SUBSCRIBE_ADDRESS, tenantId, deviceId);
        setupProtocolAdapter(() -> helper.createEventConsumer(tenantId, msg -> {
            // expect empty notification with TTD -1
            ctx.assertEquals(EventConstants.CONTENT_TYPE_EMPTY_NOTIFICATION, msg.getContentType());
            final TimeUntilDisconnectNotification notification = TimeUntilDisconnectNotification.fromMessage(msg).orElse(null);
            log.info("received notification [{}]", notification);
            ctx.assertNotNull(notification);
            if (notification.getTtd() == -1) {
                notificationReceived.complete();
            }
        })).compose(sender -> {
            this.sender = sender;
            return null;
        }).compose(ok -> subscribeToCommands(sourceAddress, (delivery, msg) -> {
            final ResourceIdentifier address = ResourceIdentifier.fromString(msg.getReplyTo());
            if (CommandConstants.COMMAND_ENDPOINT.equals(address.getEndpoint())) {
                // extract command and request ID
                final String commandRequestId = address.getResourcePath()[4];
                final String command = msg.getSubject();
                log.trace("received command [name: {}, req-id: {}]", command, commandRequestId);
                // send response
                final String responseAddress = String.format(TEMPLATE_COMMAND_RESPONSE_ADDRESS, commandRequestId, HttpURLConnection.HTTP_OK);
                final Async messageSent = ctx.async();
                final Message message = ProtonHelper.message(command + ": ok");
                message.setAddress(responseAddress);
                sender.send(message, remoteDelivery -> {
                    ctx.assertTrue(Accepted.class.isInstance(remoteDelivery.getRemoteState()));
                    messageSent.complete();
                });
                messageSent.await();
            }
        })).setHandler(ctx.asyncAssertSuccess(ok -> setup.complete()));
        setup.await();
        notificationReceived.await();
    }
    
    /**
     * Create a test specific message consumer for consuming command messages from the AMQP adapter.
     * 
     * @param sourceAddress The source address from which command messages will be consumed.
     * @return A future succeeeding with the created sender.
     */
    private Future<Void> subscribeToCommands(final String sourceAddress, final ProtonMessageHandler msgHandler) {
        if (context == null) {
            throw new IllegalStateException("not connected");
        }
        final Future<Void> result = Future.future();
        context.runOnContext(action -> {
            final ProtonReceiver receiver = connection.createReceiver(sourceAddress);
            receiver.handler(msgHandler);
            receiver.openHandler(remoteAttach -> {
               if (remoteAttach.succeeded()) {
                   // link is opened -> subscription succeeded.
                   result.complete();
               } else {
                   result.fail(remoteAttach.cause());
               }
            });
            receiver.closeHandler(remoteClose -> {
                if (remoteClose.failed()) {
                    // If the adapter closes the link with error, then an AmqpCommandSubscription could not be created
                    // from the link's source address
                    result.fail(remoteClose.cause());
                }
            });
            receiver.open();
        });
        return result;
    }
    
    private Future<ProtonSender> setupProtocolAdapter(final Supplier<Future<MessageConsumer>> consumerFactory) {
        final String username = IntegrationTestSupport.getUsername(deviceId, tenant.getTenantId());
        return helper.registry
                .addDeviceForTenant(tenant, deviceId, password)
                .compose(ok -> consumerFactory.get())
                .compose(ok -> connectToAdapter(username, password))
                .compose(conn -> createProducer(new Target())).recover(t -> {
                   log.error("error setting up AMQP protocol adapter", t);
                   return Future.failedFuture(t);
                });
    }

}
