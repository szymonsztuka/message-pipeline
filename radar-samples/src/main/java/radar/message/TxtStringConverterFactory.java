package radar.message;

public class TxtStringConverterFactory implements StringConverterFactory {

    @Override
    public StringConverter getStringConverter(String type) {
        return new TxtEncoder();
    }
}
