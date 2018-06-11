package com.kafka.consumer.avro;

import java.io.ByteArrayInputStream;
import java.util.Map;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroValueDeserializer implements Deserializer<GenericRecord> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public GenericRecord deserialize(String topic, byte[] data) {

		GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
		GenericRecord record = null;
		try (ByteArrayInputStream bis = new ByteArrayInputStream(data)) {
			BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(bis, null);
			record = datumReader.read(null, binaryDecoder);

			return record;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return record;
	}

	@Override
	public void close() {

	}
}
