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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.apache.avro.generic.GenericContainer;

import com.financialforce.orizuru.transport.rabbitmq.exception.MessagingException;
import com.financialforce.orizuru.transport.rabbitmq.interfaces.IMessageQueue;

public class MessageQueue<I extends GenericContainer, O extends GenericContainer> implements IMessageQueue<I, O> {

	private static final String CLOUDAMQP_URL = "CLOUDAMQP_URL";

	private ConnectionFactory factory;

	public MessageQueue(ConnectionFactory factory) {
		this.factory = factory;
	}

	public Channel createChannel() throws MessagingException {

		try {

			factory.setUri(System.getenv(CLOUDAMQP_URL));
			Connection conn = factory.newConnection();
			return conn.createChannel();

		} catch (Exception ex) {
			throw new MessagingException("Failed to create channel", ex);
		}

	}

	/* (non-Javadoc)
	 * @see com.financialforce.orizuru.transport.rabbitmq.interfaces.IMessageQueue#consume(java.lang.String, com.rabbitmq.client.Channel, com.financialforce.orizuru.transport.rabbitmq.DefaultConsumer)
	 */
	@Override
	public void consume(String consumerTag, Channel channel, DefaultConsumer<I, O> consumer) throws MessagingException {

		try {
			channel.basicConsume(consumer.getQueueName(), true, consumerTag, consumer);
		} catch (Exception ex) {
			throw new MessagingException("Failed to consume message", ex);
		}

	}

}
