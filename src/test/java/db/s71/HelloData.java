package db.s71;

import java.util.Objects;

public class HelloData {
    String text;

    HelloData() {}
    HelloData(String text) { this.text = text; }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HelloData helloData = (HelloData) o;
        return Objects.equals(text, helloData.text);
    }

    @Override
    public int hashCode() {
        return Objects.hash(text);
    }
}
