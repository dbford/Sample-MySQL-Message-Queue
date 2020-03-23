package db.s71;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public class MySqlMessageQueueTest {
    static DataSource ds;
    static MessageQueue queue;
    static ObjectMapper jsonAdapter = new ObjectMapper();
    static String clientName;

    @BeforeClass
    public static void setup() throws Exception {
        Properties props = new Properties();
        props.load(MySqlMessageQueueTest.class.getClassLoader().getResourceAsStream("mysql.properties"));
        String url = props.getProperty("connectionString");

        if (url == null || url.trim().isEmpty()) {
            throw new Exception("Must put a 'connectionString' property in mysql.properties file");
        }

        ds = new MysqlConnectionPoolDataSource();
        ((MysqlDataSource) ds).setUrl(url);

        queue = new MySqlMessageQueue(ds);
        clientName = ((MySqlMessageQueue) queue).getClientName();
    }


    @Before
    public void emptyMessageQueue() throws SQLException {
        try (Connection cn = ds.getConnection();
             PreparedStatement truncate = cn.prepareStatement("truncate table s71.msg_queue"))
        {
            truncate.executeUpdate();
        }
    }

    @Test
    public void when_PushMessage_given_NoConditions_then_PushMessage() throws Exception {
        Message msg = newTestMessage();

        queue.push(msg);

        Message check = queue.getMessage(msg.getId());
        System.out.println(check);
        assertNotNull(check);
        assertSurfacePropsSame(msg, check, HelloData.class);
        assertEquals(clientName, check.getPushedBy());
        assertNotNull(check.getPushedOn());
    }

    @Test
    public void when_PeekMessage_given_NoMessages_then_ReturnEmptyList() throws IOException {
        assertTrue(queue.peek(10).isEmpty());
    }

    @Test
    public void when_PeekMessage_given_MessagesExist_and_DiffType_then_ReturnEmptyList() throws IOException {
        Message msg = newTestMessage("push_test");
        queue.push(msg);

        List<Message> checks = queue.peek("a different type", 10);
        assertEquals(0, checks.size());
    }

    @Test
    public void when_PeekMessage_given_MessagesExist_then_ReturnMessages() throws IOException {
        Message msg = newTestMessage("push_test");
        queue.push(msg);

        List<Message> checks = queue.peek(10);
        assertEquals(1, checks.size());
        assertSurfacePropsSame(msg, checks.get(0), HelloData.class);
    }

    @Test
    public void when_PeekMessage_given_MessagesExists_and_SameType_then_ReturnMessages() throws IOException {

        Message msg = newTestMessage("push_test");
        queue.push(msg);

        List<Message> checks = queue.peek("push_test", 10);
        assertEquals(1, checks.size());
        assertSurfacePropsSame(msg, checks.get(0), HelloData.class);

        //also check if the status is still pushed
        Message check = queue.getMessage(checks.get(0).getId());
        assertEquals(Message.Status.PUSHED, check.getStatus());
    }

    @Test
    public void when_PopMessage_given_NoMessages_then_ReturnEmptyList() {
        assertTrue(queue.pop(10, 120).isEmpty());
    }

    @Test
    public void when_PopMessage_given_DiffTypeFilter_then_ReturnEmptyList() throws IOException {
        queue.push(newTestMessage("pop_test"));
        assertTrue(queue.pop("a different type", 10, 120).isEmpty());
    }

    @Test
    public void when_PopMessage_given_MessagesExists_then_ReturnPoppedMessages() throws IOException {
        Message msg = newTestMessage();

        queue.push(msg);

        Message check = queue.pop(120);
        assertSurfacePropsSame(msg, check, HelloData.class);

        //also check some other basic properties
        check = queue.getMessage(check.getId());
        assertEquals(Message.Status.POPPED, check.getStatus());
        assertTrue(Duration.between(Instant.now(), check.getReadyAfter()).getSeconds() > 100);
        assertEquals(clientName, check.getPoppedBy());
        assertTrue(check.getPoppedOn().equals(check.getPushedOn()) || check.getPoppedOn().isAfter(check.getPushedOn()));
    }

    @Test
    public void when_PopMessage_given_TtlNotExpired_then_ReturnNull() throws Exception {
        Message msg = newTestMessage();
        queue.push(msg);

        System.out.println(queue.getMessage(msg.getId()));

        Message check = queue.pop(2);
        assertNotNull(check);
        assertSurfacePropsSame(msg, check, HelloData.class);

        System.out.println(queue.getMessage(msg.getId()));

        //can't be popped again until ttl is expired
        assertNull(queue.pop(2));
    }

    @Test
    public void when_PopMessage_given_TtlExpired_then_ReturnMessageAgain() throws Exception {
        //pop a message with a ttl of 2
        Message msg = newTestMessage();
        queue.push(msg);
        Message check = queue.pop(2);
        assertSurfacePropsSame(msg, check, HelloData.class);

        //wait for ttl to expire
        Thread.sleep(2100);

        //should be able to pop again
        check = queue.pop(2);
        assertSurfacePropsSame(msg, check, HelloData.class);
    }

    @Test
    public void when_PopMessage_given_Confirmed_then_ReturnMessageOnce() throws Exception {
        //push message
        Message msg = newTestMessage();
        queue.push(msg);

        //pop a message with a ttl of 2
        Message check = queue.pop(2);
        assertSurfacePropsSame(msg, check, HelloData.class);

        //confirm message
        assertEquals(1, queue.confirm(check));

        Thread.sleep(2500);

        check = queue.pop(2);
        assertNull(check);
    }

    @Test
    public void when_ConfirmMessage_given_MessagePopped_then_ConfirmMessage() throws Exception {
        Message msg = newTestMessage();
        queue.push(msg);

        Message check = queue.pop(2);
        assertSurfacePropsSame(msg, check, HelloData.class);

        assertEquals(1, queue.confirm(check));

        check = queue.getMessage(check.getId());
        assertEquals(Message.Status.CONFIRMED, check.getStatus());
        assertNull(check.getErrorMessage());
        assertTrue(check.getConfirmedOn().equals(check.getPoppedOn()) || check.getConfirmedOn().isAfter(check.getPoppedOn()));
    }

    @Test
    public void when_ConfirmMessage_given_TtlExpired_then_DoNotConfirmMessage() throws Exception {

        Message msg = newTestMessage();
        queue.push(msg);

        Message check = queue.pop(1);
        assertSurfacePropsSame(msg, check, HelloData.class);

        Thread.sleep(2000);

        assertEquals(0, queue.confirm(check));
    }

    @Test
    public void when_ConfirmMessage_given_NotPopped_then_DoNotConfirmMessage() throws Exception {
        Message msg = newTestMessage();
        queue.push(msg);

        assertEquals(0, queue.confirm(msg));
    }

    @Test
    public void when_ConfirmMessage_given_ErrorMessage_then_SaveErrorMessage() throws Exception {
        Message msg = newTestMessage();
        queue.push(msg);
        queue.pop(120);

        msg.setErrorMessage("test error");
        assertEquals(1, queue.confirm(msg));

        Message check = queue.getMessage(msg.getId());
        assertEquals(msg.getErrorMessage(), check.getErrorMessage());
    }

    @Test
    public void when_GetLength_given_NoMessages_then_ReturnZero() throws Exception {
        assertEquals(0, queue.getQueueLength());
    }

    @Test
    public void when_GetLength_given_AllMessagesPopped_then_ReturnZero() throws Exception {
        pushMessages(3);

        List<Message> msgs = queue.pop(10, 120);
        assertEquals(3, msgs.size());

        assertEquals(0, queue.getQueueLength());
    }

    @Test
    public void when_GetLength_given_SomeMessagesPopped_then_ReturnPeekedMessageCount() throws Exception {
        pushMessages("get_length_test", 6);

        List<Message> msgs = queue.pop(3, 120);
        assertEquals(3, msgs.size());

        assertEquals(3, queue.getQueueLength());
        assertEquals(0, queue.getQueueLength("wrong message type"));
        assertEquals(3, queue.getQueueLength("get_length_test"));

        assertEquals(6, queue.getQueueLength(true));
        assertEquals(0, queue.getQueueLength("wrong message type", true));
        assertEquals(6, queue.getQueueLength("get_length_test", true));

        assertEquals(3, queue.confirm(msgs));
        assertEquals(3, queue.getQueueLength());
        assertEquals(3, queue.getQueueLength(true));
    }

    @Test
    @Ignore //use this if you want to generate a large amount of test data
    public void generateMessages() throws Exception {

        List<Message> batch = new ArrayList<>(10_000);
        for (int i = 0; i < 10_000; ++i) {
            batch.add(newTestMessage());
        }

        System.out.println("start");
        long start = System.currentTimeMillis();
        for (int i = 1_000_000; i > 0; i -= batch.size()) {
            queue.push(batch);

            if (i % 10_000 == 0) {
                System.out.println(i);
            }
        }
        long delta = System.currentTimeMillis() - start;
        System.out.println("took " + delta);
    }

    @Test(timeout = 240_000)
    @Ignore //use this test to play around with batch sizes, number of consumers, etc
    public void testThroughput() throws Exception {
        //fiddle with these flags as wanted
        final int NUM_MSG = 100_000;

        final int NUM_PRODUCERS = 4;
        final int NUM_CONSUMERS = 8;

        final int PRODUCE_BATCH_SIZE = 1000;
        final int CONSUME_BATCH_SIZE = 500;

        final int PRODUCER_HEAD_START_MILLIS = 2000;

        final int CHECKPOINT = NUM_MSG / 10;
        final int PROD_CHECKPOINT = CHECKPOINT / NUM_PRODUCERS;
        final int CONS_CHECKPOINT = CHECKPOINT / NUM_CONSUMERS;




        final CountDownLatch doneFlags = new CountDownLatch(NUM_PRODUCERS + NUM_CONSUMERS);
        final CountDownLatch producerDoneFlags = new CountDownLatch(NUM_PRODUCERS);
        final AtomicReference<Exception> produceException = new AtomicReference<>();
        final AtomicReference<Exception> consumeException = new AtomicReference<>();
        final AtomicInteger totalNumConsumed = new AtomicInteger(0);
        final AtomicInteger totalNumProduced = new AtomicInteger(0);


        final Runnable producer = () -> {
            long start = System.currentTimeMillis();
            System.out.println("P: Start");

            int i = 0;
            try {
                int nextCheckpoint = PROD_CHECKPOINT;

                //produce messages
                List<Message> batch = new ArrayList<>(PRODUCE_BATCH_SIZE);
                for (int z = 0; z < PRODUCE_BATCH_SIZE; ++z) {
                    batch.add(newTestMessage("produce"));
                }

                for (; i < NUM_MSG / NUM_PRODUCERS; i += batch.size()) {
                    queue.push(batch);

                    if (i >= nextCheckpoint) {
                        System.out.println("P: " + i);
                        nextCheckpoint += PROD_CHECKPOINT;
                    }
                }

                System.out.println("P: " + i);

            } catch (Exception e) {
                produceException.set(e);
                e.printStackTrace();
            } finally {
                long delta = (System.currentTimeMillis() - start);
                System.out.println("P: done in " + delta);
                System.out.println("P: " + ((double)i / (double)delta * 1000.0) + "msg/sec");

                totalNumProduced.addAndGet(i);

                producerDoneFlags.countDown();
                doneFlags.countDown();
            }
        };

        for (int i = 0; i < NUM_PRODUCERS; ++i) {
            new Thread(producer).start();
        }

        //careful here, perf degrades when producer is far ahead of consumer
        Thread.sleep(PRODUCER_HEAD_START_MILLIS); //given producer thread a head start

        final Runnable consumer = () -> {
            long start = System.currentTimeMillis();
            System.out.println("C: Start");

            int i = 0;
            try {
                int nextCheckpoint = CONS_CHECKPOINT;

                int lastPopped = 1;
                while (producerDoneFlags.getCount() > 0 || lastPopped > 0) {
                    List<Message> msgs = queue.pop(CONSUME_BATCH_SIZE, 600);
                    lastPopped = msgs.size();
                    i +=  queue.confirm(msgs);
//                    i += msgs.size();

                    if (lastPopped == 0) {
                        Thread.sleep(15);
                    }

                    if (i >= nextCheckpoint) {
                        System.out.println("C: " + i);

                        nextCheckpoint += CONS_CHECKPOINT;
                    }
                }

                System.out.println("C: " + i);
            } catch (Exception e) {
                consumeException.set(e);
                e.printStackTrace();
            } finally {
                long delta = (System.currentTimeMillis() - start);
                System.out.println("C: done in " + delta);
                System.out.println("C: " + ((double)i / (double)delta * 1000.0) + "msg/sec");

                totalNumConsumed.addAndGet(i);

                doneFlags.countDown();
            }
        };

        for (int c = 0; c < NUM_CONSUMERS; ++c) {
            (new Thread(consumer)).start();
        }

        doneFlags.await();
        System.out.println("# Deadlocks: " + ((MySqlMessageQueue) queue).getDeadlockCount());
        System.out.println("Total num produced: " + totalNumProduced.get());
        System.out.println("Total num consumed: " + totalNumConsumed.get());
        System.out.println("all done");

        assertNull(produceException.get());
        assertNull(consumeException.get());

        assertEquals(0, queue.getQueueLength(true));
    }

    private List<Message> pushMessages(int numMessages) throws JsonProcessingException {
        return pushMessages(null, numMessages);
    }

    private List<Message> pushMessages(String type, int numMessages) throws JsonProcessingException {
        List<Message> msg = new ArrayList<>(numMessages);

        for (int i = 0; i < numMessages; ++i) {
            if (type == null) {
                msg.add(newTestMessage());
            } else {
                msg.add(newTestMessage(type));
            }
        }

        queue.push(msg);

        return msg;
    }

    private void setData(Message msg, Object data) throws JsonProcessingException {
        msg.setData(jsonAdapter.writeValueAsString(data));
    }

    private <T> void assertSurfacePropsSame(Message a, Message b, Class<T> dataType) throws IOException {
        assertEquals(a, b);
        assertEquals(a.getType(), b.getType());
        assertDataEquals(a, b, dataType);
    }

    private <T> void assertDataEquals(Message a, Message b, Class<T> dataType) throws IOException {
        if (a.getData() != null && b.getData() != null) {
            T aData = jsonAdapter.readValue(a.getData(), dataType);
            T bData = jsonAdapter.readValue(b.getData(), dataType);

            assertEquals(aData, bData);

        } else if (a.getData() != null || b.getData() != null) {
            fail("Expected both data values to be null or non-null, both one is null and the other is non-null");
        } else {
            //both values are null - no need to do anything here
        }
    }

    private Message newTestMessage() throws JsonProcessingException {
        final String type = "test_" + Math.random();

        return newTestMessage(type);
    }

    private Message newTestMessage(String type) throws JsonProcessingException {
        final HelloData data = new HelloData("hello, world!" + Math.random());

        Message msg = new Message();
        msg.setType(type);
        setData(msg, data);

        return msg;
    }
}