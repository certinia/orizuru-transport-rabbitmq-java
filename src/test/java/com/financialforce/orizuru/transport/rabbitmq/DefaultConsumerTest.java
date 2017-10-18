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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;

import com.rabbitmq.client.Channel;

import org.apache.avro.generic.GenericContainer;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.financialforce.orizuru.exception.consumer.handler.HandleMessageException;
import com.financialforce.orizuru.exception.publisher.OrizuruPublisherException;
import com.financialforce.orizuru.interfaces.IPublisher;
import com.financialforce.orizuru.message.Context;

public class DefaultConsumerTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

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
	
	@Test
	public void handleDelivery_shouldConsumeTheIncomingMessage() throws Exception {

		// given
		byte[] body = Base64.getDecoder().decode(getFileContents("validTransport.txt"));
		
		Channel channel = mock(Channel.class);
		TestConsumer consumer = new TestConsumer(channel, "input", null);
		
		// when
		consumer.handleDelivery("test", null, null, body);

		// then
		verify(channel, never()).basicPublish(any(), any(), any(), any());

	}
	
	@Test
	public void handleDelivery_shouldConsumeTheIncomingMessageAndCreateAnOutgoingMessage() throws Exception {

		// given
		byte[] body = Base64.getDecoder().decode(getFileContents("validTransport.txt"));
		
		Channel channel = mock(Channel.class);
		IPublisher publisher = mock(IPublisher.class);
		TestConsumer consumer = new TestConsumer(channel, "input", null);
		consumer.setPublisher(publisher);
		
		// when
		consumer.handleDelivery("test", null, null, body);

		// thens
		verify(publisher, times(1)).publish(any(), any());
		
	}
	
	@Test
	public void handleDelivery_shouldConsumeTheIncomingMessageAndPublishOutgoingMessage() throws Exception {

		// given
		byte[] body = Base64.getDecoder().decode(getFileContents("validTransport.txt"));
		
		Channel channel = mock(Channel.class);
		TestConsumer consumer = new TestConsumer(channel, "input", "output");
		
		// when
		consumer.handleDelivery("test", null, null, body);

		// then
		verify(channel, times(1)).basicPublish(any(), any(), any(), any());
		
	}
	
	@Test
	public void handleDelivery_shouldThrowAnOrizuruPublisherExceptionIfPublishingFails() throws Exception {

		// given
		byte[] body = Base64.getDecoder().decode(getFileContents("validTransport.txt"));
		
		Channel channel = mock(Channel.class);
		ErrorConsumer consumer = new ErrorConsumer(channel, "input", "output");
		
		// expect
		exception.expect(IOException.class);
		exception.expectMessage("Failed to consume message");
		exception.expectCause(IsInstanceOf.<Throwable>instanceOf(OrizuruPublisherException.class));
				
		// when
		consumer.handleDelivery("test", null, null, body);
		
	}
	
	@Test
	public void handleDelivery_shouldThrowAnOrizuruExceptionForAnInvalidMessage() throws Exception {

		// given
		byte[] body = "test".getBytes();
		
		Channel channel = mock(Channel.class);
		TestConsumer consumer = new TestConsumer(channel, "input", null);
		
		// expect
		exception.expect(IOException.class);
		exception.expectMessage("Failed to consume message");
		
		// when
		consumer.handleDelivery("test", null, null, body);

	}

	private byte[] getFileContents(String fileName) throws IOException {

		ByteArrayOutputStream output = null;

		try {

			InputStream input = getClass().getResourceAsStream(fileName);

			output = new ByteArrayOutputStream();

			byte[] buffer = new byte[8192];
			int n = 0;
			while (-1 != (n = input.read(buffer))) {
				output.write(buffer, 0, n);
			}

			return output.toByteArray();

		} catch (IOException ioe) {
			if (output != null) {
				output.close();
			}
		}
		
		return null;
	}

	
	private class TestConsumer extends DefaultConsumer<GenericContainer, GenericContainer> {

		public TestConsumer(Channel channel, String incomingQueueName, String outgoingQueueName) {
			super(channel, incomingQueueName, outgoingQueueName);
		}

		@Override
		public GenericContainer handleMessage(Context context, GenericContainer input) throws HandleMessageException {
			return input;
		}
	
	}
	
	private class ErrorConsumer extends DefaultConsumer<GenericContainer, GenericContainer> {

		public ErrorConsumer(Channel channel, String incomingQueueName, String outgoingQueueName) {
			super(channel, incomingQueueName, outgoingQueueName);
		}

		@Override
		public GenericContainer handleMessage(Context context, GenericContainer input) throws HandleMessageException {
			return null;
		}
	
	}

}
