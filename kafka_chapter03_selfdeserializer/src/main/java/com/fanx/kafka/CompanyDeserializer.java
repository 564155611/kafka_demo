package com.fanx.kafka;

import com.sun.xml.internal.ws.encoding.soap.DeserializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class CompanyDeserializer implements Deserializer<Company> {
    private String encoding = StandardCharsets.UTF_8.toString();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.serializer.encoding" : "value.serializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("serializer.encoding");
        if (encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        if (data.length < 8) {
            throw new DeserializationException("Size of data received is shorter than excepted:8");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;
        String name, address;
        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);
        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        try {
            name = new String(nameBytes, encoding);
            address = new String(addressBytes, encoding);
        } catch (UnsupportedEncodingException e) {
            throw new DeserializationException("Error occur when deserializing!");
        }
        return new Company(name, address);
    }

    @Override
    public void close() {

    }
}
