package com.kafka.producer.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serializer;

public class AvroValueSerializer implements Serializer<GenericRecord> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public byte[] serialize(String topic, GenericRecord datum) {

		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(datum.getSchema());
		byte[] byteData = null;
		try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
			Encoder e = EncoderFactory.get().binaryEncoder(os, null);
			writer.write(datum, e);
			e.flush();
			byteData = os.toByteArray();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		return byteData;
	}

	@Override
	public void close() {

	}

}
