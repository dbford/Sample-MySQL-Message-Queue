package db.s71;

import com.mysql.cj.exceptions.DeadlockTimeoutRollbackMarker;

import javax.sql.DataSource;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MySql implementation of the MessageQueue API
 */
public class MySqlMessageQueue implements MessageQueue {

    private static String getDefaultClientName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException he) {
            return "Unknown";
        }
    }

    private final DataSource ds;
    private final String clientName;

    private final AtomicInteger popDeadlockCount = new AtomicInteger(0);
    private final AtomicInteger confirmDeadlockCount = new AtomicInteger(0);

    public MySqlMessageQueue(DataSource ds, String clientName) {
        this.ds = ds;
        this.clientName = clientName;
    }

    public MySqlMessageQueue(DataSource ds) {
        this(ds, getDefaultClientName());
    }

    public DataSource getDs() {
        return ds;
    }

    public String getClientName() {
        return clientName;
    }

    public int getDeadlockCount() {
        return popDeadlockCount.get() + confirmDeadlockCount.get();
    }

    private static final String SQL_PUSH =
            "insert into s71.msg_queue " +
                   "(msg_type, pushed_by, msg_data) " +
            "values (?, ?, ?) ";

    @Override
    public boolean[] push(List<Message> messages) {
        try (Connection cn = ds.getConnection();
             PreparedStatement insert = cn.prepareStatement(SQL_PUSH, Statement.RETURN_GENERATED_KEYS))
        {
            cn.setAutoCommit(false);
            try {

                boolean[] result = new boolean[messages.size()];
                int i = 0;

                for (Message message: messages) {
                    try {
                        insert.setString(1, message.getType());
                        insert.setString(2, this.clientName);
                        insert.setString(3, message.getData());
                        insert.execute();

                        ResultSet gen = insert.getGeneratedKeys();
                        gen.next();
                        message.setId(gen.getLong(1));

                        result[i++] = true;

                        if (i % 10_000 == 0) {
                            cn.commit();
                        }

                    } catch (SQLException se) {
                        result[i] = false;
                    }
                }

                return result;

            } finally {
                cn.setAutoCommit(true);
            }

        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    private static final String BASE_SQL_PEEK =
            "select msg_id, msg_type, ready_after, msg_data " +
            "from s71.msg_queue mq " +
            "where (mq.msg_status in ('PUSHED', 'POPPED')) " +
              "and (mq.ready_after <= current_timestamp) " +
              "and (? is null or ? = mq.msg_type) " +
            "order by mq.ready_after asc " +
            "limit ? ";

    private static final String SQL_PEEK_NOLOCK =
            BASE_SQL_PEEK +
            "for share skip locked ";

    private static final String SQL_PEEK_LOCK =
            BASE_SQL_PEEK +
            "for update skip locked ";

    private List<Message> doPeek(String query, Connection cn, String messageType, int limit) throws SQLException {
        try (PreparedStatement ps = cn.prepareStatement(query)) {
            ps.setString(1, messageType);
            ps.setString(2, messageType);
            ps.setInt(3, limit);

            List<Message> msgs = new ArrayList<>(limit);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Message msg = new Message();
                    msg.setId(rs.getInt(1));
                    msg.setType(rs.getString(2));
                    msg.setReadyAfter(rs.getTimestamp(3).toInstant());
                    msg.setData(rs.getString(4));

                    msgs.add(msg);
                }
            }

            return msgs;
        }
    }

    @Override
    public List<Message> peek(String messageType, int limit) {
        try (Connection cn = ds.getConnection()) {

            return doPeek(SQL_PEEK_NOLOCK, cn, messageType, limit);

        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    private static final int POP_BATCH_SIZE = 100;

    private static final String SQL_POP =
            "update s71.msg_queue " +
            "set msg_status = 'POPPED', " +
                "popped_on = current_timestamp, " +
                "popped_by = ?, " +
                "ready_after = date_add(current_timestamp, interval ? second) " +
            "where msg_id in (?" + repeatString(",?", POP_BATCH_SIZE - 1) + ") ";

    @Override
    public List<Message> pop(String messageType, int limit, int ttlSeconds) {
        try (Connection cn = ds.getConnection();
             Statement xa = cn.createStatement())
        {
            cn.setAutoCommit(false);
//            xa.execute("set transaction isolation level read committed");
            xa.execute("start transaction ");

            try {
                List<Message> messages = doPeek(SQL_PEEK_LOCK, cn, messageType, limit);

                try (PreparedStatement update = cn.prepareStatement(SQL_POP)) {
                    update.setString(1, clientName);
                    update.setInt(2, ttlSeconds);

                    int i = 1;

                    for (Message msg: messages) {
                        update.setLong(i + 2, msg.getId());

                        //every N batch size,
                        if (i++ % POP_BATCH_SIZE == 0) {
                            update.executeUpdate();

                            i = 1;
                        }
                    }

                    //do final batch
                    if (i != 1) {
                        for (; i <= POP_BATCH_SIZE; ++i) {
                            update.setLong(i + 2, -100);
                        }
                        update.executeUpdate();
                    }

                    cn.commit();
                }

                return messages;
            } finally {
                cn.setAutoCommit(true);
            }
        } catch (SQLException se) {
            if (se instanceof SQLTransactionRollbackException && se instanceof DeadlockTimeoutRollbackMarker) {
                popDeadlockCount.incrementAndGet();

                //don't try again here - just return zero and count on the caller trying again later
                return Collections.emptyList();
            } else {
                throw new RuntimeException(se);
            }

        }
    }

    private static final String SQL_CONFIRM =
            "update s71.msg_queue " +
            "set confirmed_on = current_timestamp," +
                "msg_status = 'CONFIRMED'," +
                "error_message = ? " +
            "where msg_id = ? " +
              "and msg_status = 'POPPED' " +
              "and ready_after > current_timestamp ";

    @Override
    public int confirm(List<Message> messages) {
        try (Connection cn = ds.getConnection();
             Statement xa = cn.createStatement())
        {
            cn.setAutoCommit(false);
            try {
//                xa.execute("set transaction isolation level read committed");
                xa.execute("start transaction ");

                //start by setting the confirmed status on all messages
                int numUpdated = 0;
                try (PreparedStatement update = cn.prepareStatement(SQL_CONFIRM)) {
                    for (Message msg: messages) {
                        update.setString(1, msg.getErrorMessage());
                        update.setLong(2, msg.getId());

                        numUpdated += update.executeUpdate();
                    }
                }

                cn.commit();

                return numUpdated;
            } finally {
                cn.setAutoCommit(true);
            }

        } catch (SQLException se) {
            if (se instanceof SQLTransactionRollbackException && se instanceof DeadlockTimeoutRollbackMarker) {
                confirmDeadlockCount.incrementAndGet();

                //try again here - want to rely on the return value to check for this kind of error
                return confirm(messages);
            } else {
                throw new RuntimeException(se);
            }
        }
    }

    private static final String SQL_QUEUE_LEN =
            "select count(*) " +
            "from s71.msg_queue mq " +
            "where (mq.msg_status in ('PUSHED', 'POPPED')) " +
              "and ((? = 0 and current_timestamp >= mq.ready_after) or (? <> 0)) " +
              "and (? is null or ? = mq.msg_type) ";

    @Override
    public long getQueueLength(String messageType, boolean includeHidden) {
        try (Connection cn = ds.getConnection();
            PreparedStatement select = cn.prepareStatement(SQL_QUEUE_LEN))
        {
            int hiddenFlag = includeHidden ? 1 : 0;
            select.setInt(1, hiddenFlag);
            select.setInt(2, hiddenFlag);

            select.setString(3, messageType);
            select.setString(4, messageType);

            try (ResultSet rs = select.executeQuery()) {
                rs.next();

                return rs.getLong(1);
            }
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    private static final String SQL_GET_MSG =
            "select msg_id, msg_type, msg_status, msg_data, ready_after, " +
                   "pushed_on, pushed_by, popped_on, popped_by, confirmed_on, " +
                   "error_message " +
            "from s71.msg_queue mq " +
            "where mq.msg_id = ?";


    @Override
    public Message getMessage(long id) {
        try (Connection cn = ds.getConnection();
             PreparedStatement select = cn.prepareStatement(SQL_GET_MSG))
        {
            select.setLong(1, id);

            Message msg = null;
            try (ResultSet rs = select.executeQuery()) {
                if (rs.next()) {
                    msg = new Message();
                    msg.setId(rs.getLong(1));
                    msg.setType(rs.getString(2));
                    msg.setStatus(Message.Status.valueOf(rs.getString(3)));
                    msg.setData(rs.getString(4));
                    msg.setReadyAfter(rs.getTimestamp(5).toInstant());
                    msg.setPushedOn(getInstant(rs, 6));
                    msg.setPushedBy(rs.getString(7));
                    msg.setPoppedOn(getInstant(rs, 8));
                    msg.setPoppedBy(rs.getString(9));
                    msg.setConfirmedOn(getInstant(rs, 10));
                    msg.setErrorMessage(rs.getString(11));
                }
            }

            return msg;
        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    private static final String SQL_DELETE =
            "delete from s71.msg_queue " +
            "where msg_status = 'CONFIRMED' " +
              "and pushed_on <= ?";

    /**
     * Delete confirmed messages older than a given cutoff period.
     *
     * @param cutoff Cutoff timestamp
     * @return The number of delete messages
     */
    public long deleteMessageOlderThan(Instant cutoff) {
        try (Connection cn = ds.getConnection();
             PreparedStatement delete = cn.prepareStatement(SQL_DELETE))
        {
            delete.setTimestamp(1, Timestamp.from(cutoff));
            return delete.executeLargeUpdate();

        } catch (SQLException se) {
            throw new RuntimeException(se);
        }
    }

    private static Instant getInstant(ResultSet rs, int i) throws SQLException {
        Timestamp ts = rs.getTimestamp(i);
        if (ts != null) {
            return ts.toInstant();
        } else {
            return null;
        }
    }

    private static StringBuilder repeatString(String s, int repeatCount) {
        StringBuilder buffer = new StringBuilder(s.length() * repeatCount + 1);
        while (repeatCount-- > 0) {
            buffer.append(s);
        }

        return buffer;
    }
}
