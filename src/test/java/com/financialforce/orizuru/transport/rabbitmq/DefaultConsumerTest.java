/**
 * Copyright (c) 2017, FinancialForce.com, inc
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 *   are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *      this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice,
 *      this list of conditions and the following disclaimer in the documentation
 *      and/or other materials provided with the distribution.
 * - Neither the name of the FinancialForce.com, inc nor the names of its contributors
 *      may be used to endorse or promote products derived from this software without
 *      specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 *  OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 *  THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 *  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 *  OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY
 *  OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 **/

package com.financialforce.orizuru.transport.rabbitmq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import com.rabbitmq.client.Channel;

import org.apache.avro.generic.GenericContainer;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.financialforce.orizuru.exception.OrizuruException;
import com.financialforce.orizuru.exception.consumer.handler.HandleMessageException;
import com.financialforce.orizuru.interfaces.IPublisher;
import com.financialforce.orizuru.message.Context;

public class DefaultConsumerTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Test
	public void handleDelivery_shouldCallConsume() throws Exception {

		// given
		byte[] expectedBody = "test".getBytes();

		Channel channel = mock(Channel.class);
		DefaultConsumer consumer = mock(DefaultConsumer.class);
		doCallRealMethod().when(consumer).handleDelivery(any(), any(), any(), any());

		// when 
		consumer.handleDelivery("test", null, null, expectedBody);

		// then
		verify(consumer, times(1)).consume(expectedBody);

	}

	@Test
	public void handleDelivery_shouldThrowAnIOExceptionIfTheMessageFailsToBeConsumed() throws Exception {

		// given
		byte[] expectedBody = "test".getBytes();

		Channel channel = mock(Channel.class);
		DefaultConsumer consumer = mock(DefaultConsumer.class);
		doCallRealMethod().when(consumer).handleDelivery(any(), any(), any(), any());
		when(consumer.consume(any())).thenThrow(NullPointerException.class);

		// expect
		exception.expect(IOException.class);
		exception.expectMessage("Failed to consume message");
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(NullPointerException.class));

		// when 
		consumer.handleDelivery("test", null, null, expectedBody);

	}

	@Test
	public void constructor_shouldCreateADefaultPublisherWithTheOutgoingQueueName() {

		// given
		String expectedOutgoingQueueName = "output";
		Channel channel = mock(Channel.class);

		// when
		TestConsumer consumer = new TestConsumer(channel, "input", expectedOutgoingQueueName);

		// then
		assertEquals(expectedOutgoingQueueName, consumer.getPublisher().getQueueName());

	}

	@Test
	public void getChannel_shouldReturnTheChannel() {

		// given
		Channel channel = mock(Channel.class);
		TestConsumer consumer = new TestConsumer(channel, "input", null);

		// when/then
		assertEquals(channel, consumer.getChannel());

	}

	@Test
	public void getConsumerTag_shouldReturnTheNullConsumerTag_IfNoMessageHasBeenHandled() {

		// given
		Channel channel = mock(Channel.class);
		TestConsumer consumer = new TestConsumer(channel, "input", null);

		// when/then
		assertNull(consumer.getConsumerTag());

	}

	@Test
	public void getConsumerTag_shouldReturnTheNullConsumerTag_IfHandleCancelOkHasBeenCalled() {

		// given
		String consumerTag = "consumer";
		Channel channel = mock(Channel.class);
		TestConsumer consumer = new TestConsumer(channel, "input", null);
		consumer.handleCancelOk(consumerTag);

		// when/then
		assertNull(consumer.getConsumerTag());

	}

	@Test
	public void getConsumerTag_shouldReturnTheNullConsumerTag_IfHandleCancelHasBeenCalled() throws Exception {

		// given
		String consumerTag = "consumer";
		Channel channel = mock(Channel.class);
		TestConsumer consumer = new TestConsumer(channel, "input", null);
		consumer.handleCancel(consumerTag);

		// when/then
		assertNull(consumer.getConsumerTag());

	}

	@Test
	public void getConsumerTag_shouldReturnTheNullConsumerTag_IfHandleShutdownSignalHasBeenCalled() {

		// given
		String consumerTag = "consumer";
		Channel channel = mock(Channel.class);
		TestConsumer consumer = new TestConsumer(channel, "input", null);
		consumer.handleShutdownSignal(consumerTag, null);

		// when/then
		assertNull(consumer.getConsumerTag());

	}

	@Test
	public void getConsumerTag_shouldReturnTheNullConsumerTag_IfHandleRecoverOkHasBeenCalled() {

		// given
		String consumerTag = "consumer";
		Channel channel = mock(Channel.class);
		TestConsumer consumer = new TestConsumer(channel, "input", null);
		consumer.handleRecoverOk(consumerTag);

		// when/then
		assertNull(consumer.getConsumerTag());

	}

	@Test
	public void getConsumerTag_shouldReturnTheConsumerTag_IfAMessageHasBeenHandled() {

		// given
		String expectedTag = "consumer";
		Channel channel = mock(Channel.class);
		TestConsumer consumer = new TestConsumer(channel, "input", null);
		consumer.handleConsumeOk(expectedTag);

		// when/then
		assertEquals(expectedTag, consumer.getConsumerTag());

	}

	private class TestConsumer extends DefaultConsumer<GenericContainer, GenericContainer> {

		public TestConsumer(Channel channel, String incomingQueueName, String outgoingQueueName) {
			super(channel, incomingQueueName, outgoingQueueName);
		}

		@Override
		public GenericContainer handleMessage(Context context, GenericContainer input) throws HandleMessageException {
			return input;
		}

		@Override
		public byte[] consume(byte[] body) throws OrizuruException {
			return body;
		}

		public IPublisher<GenericContainer> getPublisher() {
			return publisher;
		}

	}

}
