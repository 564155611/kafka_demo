package com.fanx.kafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

public class SerdeserializeUtil {
    public static byte[] serialize(Object o){
        Serde<?> serde = Serdes.serdeFrom(o.getClass());
        Serializer serializer = serde.serializer();
        return serializer.serialize(null, o);
    }

    public static<T> T deserialize(byte[] bytes,Class<T> clz){
        Serde<?> serde = Serdes.serdeFrom(clz);
        return (T) serde.deserializer().deserialize(null, bytes);
    }
}
