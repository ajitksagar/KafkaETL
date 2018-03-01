package com.etl.kafka;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * This producer is written to fetch the zipcodes from the orders_with_nocity_location view.
 * It uses JDBC connection to fetch the records by using SQL prepared statement.
 * After getting the zipcodes from the resultset it then sends over to consumer through Kafka broker.
 * 
 * @author Ajit.Kshirsagar
 *
 */
 
public class OrdersProducer
{
       
       private static Producer<String, String> producer;
       private static final String topic= "stored_orders";
              
       private final static String BOOTSTRAP_SERVERS = "localhost:9092";
 
        
       private static Producer<String, String> createProducer() {
           Properties props = new Properties();
           props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                               BOOTSTRAP_SERVERS);
           props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
           props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                           StringSerializer.class.getName());
           props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                       StringSerializer.class.getName());
           return new KafkaProducer<String, String>(props);
       }
       
       
       
       public void PublishRecords() throws Exception
       {
    	   
    	   	  producer = createProducer();
    	   
 
    	      // create PostgreSQL database connection
    	      
    		  String url = "jdbc:postgresql://localhost/postgres";
    		  Properties props = new Properties();
    		  props.setProperty("user","postgres");
    		  props.setProperty("password","admin1234");
    		  Connection conn = DriverManager.getConnection(url, props);
    		      		  
    	      // our SQL SELECT query. 
    	      // if you only need a few columns, specify them by name instead of using "*"
    	      String query = "SELECT * FROM orders_with_nocity_location";

    	      // create the java statement
    	      Statement st = conn.createStatement();
    	      
    	      // execute the query, and get a java resultset
    	      ResultSet rs = st.executeQuery(query);
    	      
    	      // iterate through the java resultset
    	      
    	      
    	      while (rs.next())
    	      {


    	        String zipCode = rs.getString("zipcode");
    	        
    	        // This publishes message on given topic
    	        producer.send(new ProducerRecord<String, String>(topic,zipCode));
    	             	        

    	      }
    	      conn.close();
    	      producer.close();
    	      
    	  }
    	   
             
   
       
public static void main(String[] args) throws Exception
{
			OrdersProducer kProducer = new OrdersProducer();
           
			// Publish message
			kProducer.PublishRecords();

             
}

}
