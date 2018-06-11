package com.kafka.avroexamples;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import com.avro.generated.Employee;

/**
 * Hello world!
 *
 */
public class SerializeDeserialize {
	public static void main(String[] args) {

		serializeAvro();
		deserializeAvro();
	}

	public static void serializeAvro() {
		DatumWriter<Employee> empDatumWriter = new SpecificDatumWriter<>(Employee.class);
		DataFileWriter<Employee> empDataFileWriter = new DataFileWriter<>(empDatumWriter);

		Employee emp = Employee.newBuilder().setName("Amaresh").setId(1).setAge(32).setAddress("Faridabad")
				.setSalary(1000000).build();
		try {
			empDataFileWriter.create(emp.getSchema(),
					new File("/home/amaresh/User_Drive/git_hub/Kafka-client-examples/avro-store/emp.avro"));
			empDataFileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Avro Serialization complete....");
	}

	public static void deserializeAvro() {
		DatumReader<Employee> empDatumReader = new SpecificDatumReader<>(Employee.class);

		try {
			DataFileReader<Employee> empDataFileReader = new DataFileReader<Employee>(
					new File("/home/amaresh/User_Drive/git_hub/Kafka-client-examples/avro-store/emp.avro"),
					empDatumReader);
			Employee em = null;

			while (empDataFileReader.hasNext()) {
				em = empDataFileReader.next(em);
				System.out.println(em.getName());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Avro deserialization complete....");
	}
}
