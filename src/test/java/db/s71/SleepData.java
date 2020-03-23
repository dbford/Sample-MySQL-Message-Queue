package db.s71;

import java.util.Objects;

public class SleepData {
    int numSeconds;

    SleepData() {}
    SleepData(int seconds) { this.numSeconds = seconds; }

    public int getNumSeconds() {
        return numSeconds;
    }

    public void setNumSeconds(int numSeconds) {
        this.numSeconds = numSeconds;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SleepData sleepData = (SleepData) o;
        return numSeconds == sleepData.numSeconds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(numSeconds);
    }
}
