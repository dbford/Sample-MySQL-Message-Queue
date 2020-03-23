package db.s71;

import java.time.Instant;
import java.util.Objects;

/**
 * Message class - matches the msg_queue database record.
 */
public class Message {
    public enum Status {
        PUSHED,
        POPPED,
        CONFIRMED
    }

    private long id;
    private String type;
    private Status status;
    private Instant readyAfter;
    private String data;

    private Instant pushedOn;
    private String pushedBy;

    private Instant poppedOn;
    private String poppedBy;

    private Instant confirmedOn;

    private String errorMessage;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Instant getReadyAfter() {
        return readyAfter;
    }

    public void setReadyAfter(Instant readyAfter) {
        this.readyAfter = readyAfter;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public Instant getPushedOn() {
        return pushedOn;
    }

    public void setPushedOn(Instant pushedOn) {
        this.pushedOn = pushedOn;
    }

    public String getPushedBy() {
        return pushedBy;
    }

    public void setPushedBy(String pushedBy) {
        this.pushedBy = pushedBy;
    }

    public Instant getPoppedOn() {
        return poppedOn;
    }

    public void setPoppedOn(Instant poppedOn) {
        this.poppedOn = poppedOn;
    }

    public String getPoppedBy() {
        return poppedBy;
    }

    public void setPoppedBy(String poppedBy) {
        this.poppedBy = poppedBy;
    }

    public Instant getConfirmedOn() {
        return confirmedOn;
    }

    public void setConfirmedOn(Instant confirmedOn) {
        this.confirmedOn = confirmedOn;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Message message = (Message) o;
        return id == message.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Message{" +
                "id=" + id +
                ", type='" + type + '\'' +
                ", status=" + status +
                ", readyAfter=" + readyAfter +
                ", data='" + (data != null ? "<yes>" : "<no>") + '\'' +
                ", pushedOn=" + pushedOn +
                ", pushedBy='" + pushedBy + '\'' +
                ", poppedOn=" + poppedOn +
                ", poppedBy='" + poppedBy + '\'' +
                ", confirmedOn=" + confirmedOn +
                ", errorMessage='" + (errorMessage != null ? "<yes>" : "<no>") + '\'' +
                '}';
    }
}
