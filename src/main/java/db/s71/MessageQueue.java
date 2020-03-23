package db.s71;

import java.util.Arrays;
import java.util.List;

/**
 * MessageQueue API as defined at https://gist.github.com/mikeflynn/762b2af761f26e405be3b35c6db8d00c
 */
public interface MessageQueue {
    /**
     * Pushes the given message to the queue.
     *
     * @param messages Messages to push
     * @return A list of booleans indicating whether or not each message was successfully added
     */
    boolean[] push(List<Message> messages);

    /**
     * Returns one or more messages from the queue.
     * @param messageType Optionally, a message type to filter by
     * @param limit The maximum number of messages to return
     * @return Messages from the queue.
     */
    List<Message> peek(String messageType, int limit);

    /**
     * Returns on ore messages from the queue.
     * Messages are hidden for the duration (in sec) specified
     * by the ttlSeconds ary, after which they return to the front of the queue.
     * @param messageType Optionally, a message type to filter by
     * @param limit The maximum number of messages to return
     * @param ttlSeconds The number of seconds returned messages are hidden for
     * @return Popped messages
     */
    List<Message> pop(String messageType, int limit, int ttlSeconds);


    /**
     * Deletes the given messages from the queue. This function
     * should be called to confirm the successful handling of messages
     * returned by the pop function.
     *
     * Note: differing slightly from the given design, this method only confirms messages that are in the 'POPPED'
     *       status and are within their TTL periods.
     *
     * @param messages The messages to confirm
     * @return The number of message successfully confirmed.
     */
    int confirm(List<Message> messages);

    /**
     * Returns a count of the number of messages on the queue.
     *
     * @param messageType Optionally, filter by message type.
     * @param includeHidden If true, include messages that have been popped, but not confirmed.
     * @return A count of messages
     */
    long getQueueLength(String messageType, boolean includeHidden);

    /**
     * Returns the full message with a given identifier
     * @param id ID of the message
     * @return Message with all properties
     */
    Message getMessage(long id);
    
    /* java's version of default arguments begin here. */

    /**
     * Same as {@link #getQueueLength(String, boolean)} by messageType is defaulted to null
     *
     * @see #getQueueLength(String, boolean)
     */
    default long getQueueLength(boolean includeHidden) {
        return getQueueLength(null, includeHidden);
    }

    /**
     * Same as {@link #getQueueLength(String, boolean)} by includeHidden is defaulted to false
     *
     * @see #getQueueLength(String, boolean)
     */
    default long getQueueLength(String messageType) {
        return getQueueLength(messageType, false);
    }


    /**
     * Same as {@link #getQueueLength(String, boolean)} by messageType is null and includeHidden is false
     *
     * @see #getQueueLength(String, boolean)
     */
    default long getQueueLength() {
        return getQueueLength(null, false);
    }

    /**
     * @see #push(List) 
     */
    default boolean[] push(Message... messages) {
        return push(Arrays.asList(messages));
    }

    /**
     * @see #confirm(List) 
     */
    default int confirm(Message... messages) {
        return confirm(Arrays.asList(messages));
    }
    
    
    
    /**
     * Same as {@link #peek(String, int)}, but limit is defaulted to 1
     * @see #peek(String, int)
     */
    default Message peek(String messageType) {
        List<Message> msgs = peek(messageType, 1);
        if (msgs.isEmpty()) {
            return null;
        } else {
            return msgs.get(0);
        }
    }

    /**
     * Same as {@link #peek(String, int)}, but messageType is null.
     * @see #peek(String, int)
     */
    default List<Message> peek(int limit) {
        return peek(null, limit);
    }

    /**
     * Same as {@link #peek(String, int)}, but messageType is null and limit is 1.
     * @see #peek(String, int)
     */
    default Message peek() {
        List<Message> msgs = peek(1);
        if (msgs.isEmpty()) {
            return null;
        } else {
            return msgs.get(0);
        }
    }

    /**
     * Same as {@link #pop(String, int, int)}, but limit is default to 1
     * 
     * @see #pop(String, int, int)
     */
    default List<Message> pop(String messageType, int ttlSeconds) {
        return pop(messageType, 1, ttlSeconds);
    }

    /**
     * Same as {@link #pop(String, int, int)}, but messageType is defaulted to null
     * @see #pop(String, int, int) 
     */
    default List<Message> pop(int limit, int ttlSeconds) {
        return pop(null, limit, ttlSeconds);
    }

    /**
     * Same as {@link #pop(String, int, int)}, but messageType is null and limit is 1
     * @see #pop(String, int, int) 
     */
    default Message pop(int ttlSeconds) {
        List<Message> msgs = pop(1, ttlSeconds);
        if (msgs.isEmpty()) {
            return null;
        } else {
            return msgs.get(0);
        }
    }
}
