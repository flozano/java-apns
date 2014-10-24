package com.notnoop.apns.integration;

import static com.notnoop.apns.utils.FixedCertificates.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.notnoop.apns.APNS;
import com.notnoop.apns.ApnsDelegate;
import com.notnoop.apns.ApnsNotification;
import com.notnoop.apns.ApnsService;
import com.notnoop.apns.DeliveryError;
import com.notnoop.apns.EnhancedApnsNotification;
import com.notnoop.apns.internal.netty.util.MockApnsServer;
import com.notnoop.apns.utils.FixedCertificates;
//import com.notnoop.apns.utils.MockApnsServer;

public class FixedApnsConnectionCacheTest {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(FixedApnsConnectionCacheTest.class);

    MockApnsServer server;
    
    @Rule
    public TestName testName = new TestName();
    
    @Before
    public void startup() throws InterruptedException {
        LOGGER.info("**** TEST START " + testName.getMethodName() + " ****");
        server = new MockApnsServer(TEST_GATEWAY_PORT,
                FixedCertificates.serverContext());
        server.start();
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
        server = null;
        LOGGER.info("**** TEST FINISH " + testName.getMethodName() + " ****");
    }

    @Test(timeout = 5000)
    public void test_send_50_no_failure() throws InterruptedException {
        ConnectionCacheTest test = new ConnectionCacheTest();
        test.setExpectedClosedConnections(0);
        test.setExpectedResent(0);
        test.setExpectedSent(50);
        test.setExpectedTotal(50);
        test(test);
    }

    @Test(timeout = 5000)
    public void test_20_fails_id_10_after_receiving_15()
            throws InterruptedException {
        ConnectionCacheTest test = new ConnectionCacheTest();
        test.setExpectedClosedConnections(1);
        test.setExpectedResent(5);
        test.setExpectedSent(19);
        test.setExpectedTotal(20);
        test.addFail(new Fail(10, 15, DeliveryError.MISSING_DEVICE_TOKEN));
        test(test);
    }

    @Test(timeout = 5000)
    public void test_20_fails_id_1_after_receiving_15()
            throws InterruptedException {
        ConnectionCacheTest test = new ConnectionCacheTest();
        test.setExpectedClosedConnections(1);
        test.setExpectedResent(14);
        test.setExpectedSent(19);
        test.setExpectedTotal(20);
        test.addFail(new Fail(1, 15, DeliveryError.MISSING_DEVICE_TOKEN));
        test(test);
    }

    @Test(timeout = 5000)
    public void test_20_fails_last() throws InterruptedException {
        ConnectionCacheTest test = new ConnectionCacheTest();
        test.setExpectedClosedConnections(1);
        test.setExpectedResent(0);
        test.setExpectedSent(19);
        test.setExpectedTotal(20);
        test.addFail(new Fail(19, 19, DeliveryError.MISSING_DEVICE_TOKEN));
        test(test);
    }

    @Test(timeout = 5000)
    public void test_20_fails_id_30_after_receiving_15()
            throws InterruptedException {
        ConnectionCacheTest test = new ConnectionCacheTest();
        test.setExpectedClosedConnections(1);
        test.setExpectedResent(16);
        test.setExpectedSent(15);
        test.setExpectedTotal(16);
        test.addFail(new Fail(30, 15, DeliveryError.MISSING_DEVICE_TOKEN));
        test(test);
    }

    @Test(timeout = 5000)
    public void test_20_fails_id_15_after_receiving_30()
            throws InterruptedException {
        ConnectionCacheTest test = new ConnectionCacheTest();
        test.setExpectedClosedConnections(0);
        test.setExpectedResent(0);
        test.setExpectedSent(20);
        test.setExpectedTotal(20);
        test.addFail(new Fail(15, 30, DeliveryError.MISSING_DEVICE_TOKEN));
        test(test);
    }

    @Test(timeout = 5000)
    // @Ignore
    public void test_multithread() {
        MultithreadConnectionCacheTest test = new MultithreadConnectionCacheTest();
        test.setNumOfThreads(4);
        test.setExpectedClosedConnections(1);
        test.setExpectedSent(99);
        test.setExpectedTotal(100);
        test.addFail(new Fail(55, 58, DeliveryError.MISSING_DEVICE_TOKEN));

        testMultithread(test);
    }

    @Test(timeout = 500000)
    // @Ignore
    public void test_multithread_complex() {
        MultithreadConnectionCacheTest test = new MultithreadConnectionCacheTest();
        test.setNumOfThreads(8);
        test.setExpectedClosedConnections(19);
        test.setExpectedSent(49981);
        test.setExpectedTotal(50000);
        test.addFail(new Fail(55, 58, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(259, 300, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(1024, 1035, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(5323, 5344, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(5790, 5800, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(10579, 10601, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(22359, 22370, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(27857, 27862, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(28474, 28485, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(29678, 29701, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(30865, 30872, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(32276, 32299, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(34629, 34654, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(35813, 35824, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(37666, 37715, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(38990, 38991, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(39334, 39347, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(40000, 40010, DeliveryError.MISSING_DEVICE_TOKEN));
        test.addFail(new Fail(45423, 45449, DeliveryError.MISSING_DEVICE_TOKEN));

        testMultithread(test);
    }

    @Test(timeout = 200000)
    public void test_multithread_complex_no_fails() {
        MultithreadConnectionCacheTest test = new MultithreadConnectionCacheTest();
        test.setNumOfThreads(8);
        test.setExpectedClosedConnections(0);
        test.setExpectedSent(50000);
        test.setExpectedTotal(50000);

        testMultithread(test);
    }

    protected void testMultithread(MultithreadConnectionCacheTest test) {
        final CountDownLatch sync = new CountDownLatch(test.getExpectedTotal());
        final CountDownLatch syncConnectionClosed = new CountDownLatch(test
                .getFails().size());
        final AtomicInteger numResent = new AtomicInteger();
        final AtomicInteger numSent = new AtomicInteger();
        final AtomicInteger numConnectionClosed = new AtomicInteger();

        ApnsService service = buildApnsService(new ApnsDelegate() {
            @Override
            public void messageSent(ApnsNotification message, boolean resent) {
                if (!resent) {
                    numSent.incrementAndGet();
                }
                sync.countDown();
            }

            @Override
            public void messageSendFailed(ApnsNotification message, Throwable e) {
                numSent.decrementAndGet();
            }

            @Override
            public void connectionClosed(DeliveryError e, int messageIdentifier) {
                numConnectionClosed.incrementAndGet();
                syncConnectionClosed.countDown();
            }

            @Override
            public void cacheLengthExceeded(int newCacheLength) {
            }

            @Override
            public void notificationsResent(int resendCount) {
                numResent.addAndGet(resendCount);
            }
        });

        for (Fail fail : test.getFails()) {
            server.fail(fail.errorCode, fail.idToFail, fail.failWhenReceive);
        }

        CountDownLatch syncDelivery = server.getCountDownLatch(test
                .getExpectedTotal());

        test.act(service);

        try {
//            LOGGER.trace("Waiting...");
//            Thread.sleep(40000);
//            LOGGER.trace("IDs...");
//            LOGGER.trace(server.getReceivedNotificationIds().toString());
            LOGGER.debug("Waiting for all messages to be sent");
            syncDelivery.await();
            syncConnectionClosed.await();
            sync.await();
        } catch (InterruptedException e1) {
            throw new RuntimeException(e1);
        }
        service.stop();

        Assert.assertEquals(test.getExpectedSent(), numSent.get());
        Assert.assertEquals(test.getExpectedClosedConnections(),
                numConnectionClosed.get());
        List<List<Integer>> receivedIds = server.getReceivedNotificationIds();

        Assert.assertEquals(test.getExpectedClosedConnections(),
                receivedIds.size() - 1);

        LOGGER.trace(receivedIds.toString());
        if (test.getFails().size() > 0) {
            // Check the last id in each connection is the correct when the fail
            // occurs
            for (int i = 0; i < receivedIds.size() - 1; i++) {
                Assert.assertTrue(test.getFailWhenReceiveIds().contains(
                        receivedIds.get(i).get(receivedIds.get(i).size() - 1)));
            }

            // Check all notifications have been sent
            Set<Integer> allReceivedIDs = new HashSet<Integer>();
            for (int i = 0; i < receivedIds.size(); i++) {
                allReceivedIDs.addAll(receivedIds.get(i));
            }
            Assert.assertEquals(test.getExpectedTotal(), allReceivedIDs.size());

            // Check there are not repeated notifications
            for (int i = 0; i < receivedIds.size(); i++) {
                List<Integer> ids = receivedIds.get(i);
                boolean isLast = (i == (receivedIds.size() - 1));
                int idToFail = isLast ? -1 : test.getFailByWhenReceiveID(ids
                        .get(ids.size() - 1)).idToFail;

                ArrayList<Integer> idsToRemove = new ArrayList<Integer>();
                boolean failFound = false;
                for (Integer id : ids) {
                    idsToRemove.add(id);
                    if (id == idToFail) {
                        // All notifications after that have to be resent in
                        // next connections
                        failFound = true;
                        break;
                    }
                }

                // If a fail has not been found and is not the last connection
                // it means all the notifications in the connection have to be
                // resent
                if (failFound || isLast) {
                    for (Integer id : idsToRemove) {
                        Assert.assertTrue("ID " + id
                                + " sent more than one time",
                                allReceivedIDs.remove(id));
                    }
                }
            }

            for (int id : allReceivedIDs) {
                LOGGER.error("ID " + id + " should have been removed");
            }

            Assert.assertEquals(0, allReceivedIDs.size());
        } else {
            Assert.assertEquals(1, receivedIds.size());
            Assert.assertEquals(test.getExpectedTotal(), receivedIds.get(0)
                    .size());
            for (int i = 0; i < test.getExpectedTotal(); i++) {
                Assert.assertTrue("Message " + receivedIds.get(0).get(i)
                        + " not received in list 0", receivedIds.get(0)
                        .contains(i));
            }
        }
    }

    protected void test(ConnectionCacheTest test) throws InterruptedException {
        final CountDownLatch sync = new CountDownLatch(test.getExpectedTotal());
        final CountDownLatch syncConnectionClosed = new CountDownLatch(test
                .getFails().size());
        final AtomicInteger numResent = new AtomicInteger();
        final AtomicInteger numSent = new AtomicInteger();
        final AtomicInteger numConnectionClosed = new AtomicInteger();

        ApnsService service = buildApnsService(new ApnsDelegate() {
            @Override
            public void messageSent(ApnsNotification message, boolean resent) {
                if (!resent) {
                    numSent.incrementAndGet();
                }
                sync.countDown();
            }

            @Override
            public void messageSendFailed(ApnsNotification message, Throwable e) {
                numSent.decrementAndGet();
            }

            @Override
            public void connectionClosed(DeliveryError e, int messageIdentifier) {
                numConnectionClosed.incrementAndGet();
                syncConnectionClosed.countDown();
            }

            @Override
            public void cacheLengthExceeded(int newCacheLength) {
            }

            @Override
            public void notificationsResent(int resendCount) {
                numResent.addAndGet(resendCount);
            }
        });

        for (Fail fail : test.getFails()) {
            server.fail(fail.errorCode, fail.idToFail, fail.failWhenReceive);
        }

        CountDownLatch syncDelivery = server.getCountDownLatch(test
                .getExpectedTotal());

        test.act(service);

        try {
            syncDelivery.await();
            syncConnectionClosed.await();
            sync.await();
        } catch (InterruptedException e1) {
            throw new RuntimeException(e1);
        }
        service.stop();

        Assert.assertEquals(test.getExpectedSent(), numSent.get());
        Assert.assertTrue(test.getExpectedResent() <= numResent.get());
        Assert.assertEquals(test.getExpectedClosedConnections(),
                numConnectionClosed.get());
        List<List<Integer>> receivedIds = server.getReceivedNotificationIds();

        Assert.assertEquals(test.getExpectedClosedConnections(),
                receivedIds.size() - 1);

        LOGGER.trace(receivedIds.toString());
        List<Fail> fails = test.getFails();
        if (fails.size() > 0) {
            Assert.assertEquals(test.getFails().size() + 1, receivedIds.size());

            Fail lastFail = null;
            int i = 0;
            for (; i < fails.size(); i++) {
                Fail currentFail = fails.get(i);
                int firstID = lastFail == null ? 0 : lastFail.idToFail + 1;

                int connectionSize = currentFail.failWhenReceive - firstID + 1;
                // Check the amount of notifications in the connection is
                // correct
                Assert.assertEquals(connectionSize, receivedIds.get(i).size());

                // Check the last ID in the connection is correct
                Assert.assertEquals(currentFail.failWhenReceive, receivedIds
                        .get(i).get(receivedIds.get(i).size() - 1));

                // Check all the expected notifications are in the connection
                for (int j = firstID; j <= currentFail.failWhenReceive; j++) {
                    Assert.assertTrue("Message " + j + " not received in list "
                            + i, receivedIds.get(i).contains(j));
                }

                lastFail = currentFail;
            }

            for (int j = lastFail.idToFail + 1; j < test.getExpectedTotal(); j++) {
                Assert.assertTrue(
                        "Message " + j + " not received in list " + i,
                        receivedIds.get(i).contains(j));
            }
        } else {
            Assert.assertEquals(1, receivedIds.size());
            Assert.assertEquals(test.getExpectedTotal(), receivedIds.get(0)
                    .size());
            for (int i = 0; i < test.getExpectedTotal(); i++) {
                Assert.assertTrue("Message " + i + " not received in list 0",
                        receivedIds.get(0).contains(i));
            }
        }
    }

    private ApnsService buildApnsService(ApnsDelegate apnsDelegate) {
        return APNS.newService().withSSLContext(clientContext())
                .withGatewayDestination(TEST_HOST, TEST_GATEWAY_PORT)
                .withDelegate(apnsDelegate).withCacheLength(50000).build();
    }

    public static class ConnectionCacheTest {
        protected EnhancedApnsNotification[] messages;

        private int expectedClosedConnections;
        private int expectedTotal;
        private int expectedSent;
        private int expectedResent;
        private Set<Fail> fails = new TreeSet<Fail>(new Comparator<Fail>() {

            @Override
            public int compare(Fail o1, Fail o2) {
                return o1.idToFail.compareTo(o2.idToFail);
            }
        });

        public int getExpectedClosedConnections() {
            return expectedClosedConnections;
        }

        public void setExpectedClosedConnections(int expectedClosedConnections) {
            this.expectedClosedConnections = expectedClosedConnections;
        }

        public int getExpectedTotal() {
            return expectedTotal;
        }

        public void setExpectedTotal(int expectedTotal) {
            this.expectedTotal = expectedTotal;
        }

        public int getExpectedSent() {
            return expectedSent;
        }

        public void setExpectedSent(int expectedSent) {
            this.expectedSent = expectedSent;
        }

        public int getExpectedResent() {
            return expectedResent;
        }

        public void setExpectedResent(int expectedResent) {
            this.expectedResent = expectedResent;
        }

        public List<Fail> getFails() {
            ArrayList<Fail> failList = new ArrayList<Fail>();
            for (Fail fail : fails) {
                // Ignore fails that will not cause an error
                if (fail.failWhenReceive < getExpectedTotal()) {
                    failList.add(fail);
                }
            }

            return failList;
        }

        public void addFail(Fail fail) {
            this.fails.add(fail);
        }

        public Fail getFailByIdToFail(int idToFail) {
            for (Fail fail : getFails()) {
                if (fail.idToFail == idToFail)
                    return fail;
            }

            return null;
        }

        public Fail getFailByWhenReceiveID(int failWhenReceive) {
            for (Fail fail : getFails()) {
                if (fail.failWhenReceive == failWhenReceive)
                    return fail;
            }

            return null;
        }

        public List<Integer> getFailWhenReceiveIds() {
            List<Integer> ids = new ArrayList<Integer>();
            for (Fail fail : getFails()) {
                ids.add(fail.failWhenReceive);
            }

            return ids;
        }

        protected void prepareMessages(int numMessages) {
            messages = new EnhancedApnsNotification[numMessages];

            for (int i = 0; i < numMessages; i++) {
                messages[i] = new EnhancedApnsNotification(i, 1,
                        "a87d8878d878a88", "{\"aps\":{}}");
            }
        }

        public void act(ApnsService apnsService) {
            prepareMessages(expectedTotal);

            for (int i = 0; i < expectedTotal; i++) {
                apnsService.push(messages[i]);
            }
        }

    }

    public static class MultithreadConnectionCacheTest extends
            ConnectionCacheTest {
        private int numOfThreads;

        public int getNumOfThreads() {
            return numOfThreads;
        }

        public void setNumOfThreads(int numOfThreads) {
            this.numOfThreads = numOfThreads;
        }

        @Override
        public void act(final ApnsService apnsService) {
            // Prepare notifications
            prepareMessages(getExpectedTotal());

            final ConcurrentHashMap<Integer, List<Integer>> notificationsPerThread = new ConcurrentHashMap<Integer, List<Integer>>();
            for (int i = 0; i < numOfThreads; i++) {
                notificationsPerThread.put(i, new ArrayList<Integer>());
            }

            List<Integer> failWhenReceiveIds = getFailWhenReceiveIds();
            int currentThread = 0;
            for (int id = 0; id < getExpectedTotal(); id++) {
                if (failWhenReceiveIds.contains(id)) {
                    // Look for the thread where the id to fail belongs
                    Fail fail = getFailByWhenReceiveID(id);

                    if (fail.idToFail >= id) {
                        notificationsPerThread.get(currentThread).add(id);
                        currentThread = (currentThread + 1) % numOfThreads;
                    } else {
                        for (int thread = 0; thread < numOfThreads; thread++) {
                            if (notificationsPerThread.get(thread).contains(
                                    fail.idToFail)) {
                                notificationsPerThread.get(thread).add(id);

                                if (thread == currentThread) {
                                    currentThread = (currentThread + 1)
                                            % numOfThreads;
                                }
                                break;
                            }
                        }
                    }
                } else {
                    notificationsPerThread.get(currentThread).add(id);
                    currentThread = (currentThread + 1) % numOfThreads;
                }
            }

            ExecutorService executorService = Executors
                    .newFixedThreadPool(numOfThreads);

            List<Future<?>> futures = new ArrayList<Future<?>>();
            for (int i = 0; i < numOfThreads; i++) {
                final int thread = i;
                futures.add(executorService.submit(new Runnable() {

                    @Override
                    public void run() {
                        List<Integer> notifications = notificationsPerThread
                                .get(thread);

                        for (int id : notifications) {
                            LOGGER.trace("Thread " + thread + " - ID " + id);
                            apnsService.push(messages[id]);
                        }
                    }
                }));
            }

            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }

            executorService.shutdown();
        }
    }

    private static class Fail {
        private final Integer idToFail;
        private final Integer failWhenReceive;
        private final DeliveryError errorCode;

        public Fail(int idToFail, int failWhenReceive, DeliveryError errorCode) {
            this.idToFail = idToFail;
            this.failWhenReceive = failWhenReceive;
            this.errorCode = errorCode;
        }
    }
}
