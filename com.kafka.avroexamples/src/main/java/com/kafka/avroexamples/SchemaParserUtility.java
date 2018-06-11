package com.kafka.avroexamples;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

public class SchemaParserUtility {

	public static void main(String[] args) throws IOException, URISyntaxException {
//		serialize();
		deserialize();
	}

	public static void serialize() throws IOException, URISyntaxException{
		//Instantiating the Schema.Parser class.
	      Schema schema = new Schema.Parser().parse(new File(SchemaParserUtility.class.getResource("/avro/employee.avsc").toURI()));
	      
	    //Instantiating the GenericRecord class.
	      GenericRecord e1 = new GenericData.Record(schema);
	      
	    //Insert data according to schema
	      e1.put("name", "Amar");
	      e1.put("id", 001);
	      e1.put("salary",300000);
	      e1.put("age", 32);
	      e1.put("address", "Faridabad");
	      
	      GenericRecord e2 = new GenericData.Record(schema);
			
	      e2.put("name", "rahman");
	      e2.put("id", 002);
	      e2.put("salary", 35000);
	      e2.put("age", 30);
	      e2.put("address", "Delhi");
	      
	      DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
	      DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
	      
	      dataFileWriter.create(schema, new File("/home/amaresh/User_Drive/git_hub/Kafka-client-examples/avro-store/emp2.avro"));

	      dataFileWriter.append(e1);
	      dataFileWriter.append(e2);
	      dataFileWriter.close();
			
	      System.out.println("data successfully serialized");
	      
	}
	
	public static void deserialize() throws IOException, URISyntaxException{
		
		 //Instantiating the Schema.Parser class.
	      Schema schema = new Schema.Parser().parse(new File(SchemaParserUtility.class.getResource("/avro/employee.avsc").toURI()));
	      
	      DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
	      DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File("/home/amaresh/User_Drive/git_hub/Kafka-client-examples/avro-store/emp2.avro"), datumReader);
	      GenericRecord emp = null;
	      
	      while (dataFileReader.hasNext()) {
	          emp = dataFileReader.next(emp);
	          System.out.println(emp);
	       }
	       System.out.println("Deserialization completed!!!");
	}
	
}
