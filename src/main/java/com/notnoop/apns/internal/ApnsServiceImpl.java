/*
 * Copyright 2009, Mahmood Ali.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above
 *     copyright notice, this list of conditions and the following disclaimer
 *     in the documentation and/or other materials provided with the
 *     distribution.
 *   * Neither the name of Mahmood Ali. nor the names of its
 *     contributors may be used to endorse or promote products derived from
 *     this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.notnoop.apns.internal;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.notnoop.apns.ApnsNotification;
import com.notnoop.exceptions.ApnsServiceStoppedException;
import com.notnoop.exceptions.ChannelProviderClosedException;
import com.notnoop.exceptions.NetworkIOException;

public class ApnsServiceImpl extends AbstractApnsService {

    // These two properties are used to control no more push messages are sent
    // to the connection once the service has been stopped
    private AtomicBoolean stopped = new AtomicBoolean(false);
    private ReadWriteLock rwlock = new ReentrantReadWriteLock();

    private ApnsConnection connection;

    public ApnsServiceImpl(ApnsConnection connection,
            ApnsFeedbackConnection feedback) {
        super(feedback);
        this.connection = connection;
    }

    @Override
    public void push(ApnsNotification msg) throws NetworkIOException {
        rwlock.readLock().lock();
        try {
            if (!stopped.get()) {
                connection.sendMessage(msg);
            } else {
                throw new ApnsServiceStoppedException(Utilities.encodeHex(msg
                        .getDeviceToken()));
            }
        } catch (ChannelProviderClosedException e) {
            // Unlikely to happen
            throw new ApnsServiceStoppedException(Utilities.encodeHex(msg
                    .getDeviceToken()));
        } finally {
            rwlock.readLock().unlock();
        }
    }

    public void start() {
    }

    public void stop() {
        if (!stopped.getAndSet(true)) {
            rwlock.writeLock().lock();
            try {
                Utilities.close(connection);
            } finally {
                rwlock.writeLock().unlock();
            }
        }
    }

    public void testConnection() {
        connection.testConnection();
    }
}
