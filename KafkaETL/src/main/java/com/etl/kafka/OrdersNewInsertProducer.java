package com.etl.kafka;

import java.io.DataInputStream;
import java.net.Socket;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * This Producer is written to capture the new records inserted in ORDERS Table
 * in realtime. To do this we are enabling the transaction logging in
 * PostgreSQL. We are sending the logs over the PORT: 12345 and then capturing
 * these logs through SOCKET by listening on this port. After capturing the
 * INSERT events we are processing these messages to record format and sending
 * it to OrdersNewInsertConsumer through Kafka broker.
 * 
 * @author Ajit.Kshirsagar
 *
 */

public class OrdersNewInsertProducer {

	private static Producer<String, String> producer;
	private static final String topic = "realtime_oders";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";

	/**
	 * createProducer() returns created KafkaProducer object to produce messages
	 * for consumer
	 * 
	 * @return KafkaProducer
	 */
	private static Producer<String, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<String, String>(props);
	}

	/**
	 * PublishRecords() listens to the SOCKET on PORT:12345 for transaction
	 * updates done on the orders TABLE and then processes the messages for
	 * sending it to the Kafka Consumer through Kafka Broker
	 * 
	 * @throws Exception
	 */

	@SuppressWarnings("deprecation")
	public void PublishRecords() throws Exception {

		producer = createProducer();

		String zipcode=null;

		try {

			/*
			 * Creating Socket Connection to listen on Port:12345 for further
			 * updates on orders TABLE
			 */

			Socket echoSocket = new Socket("localhost", 12345);

			// Creating Input Stream to read messages from the socket
			DataInputStream is = new DataInputStream(echoSocket.getInputStream());
			String responseLine;
			String parseLine[];

			while (true) {

				responseLine = is.readLine();
				if (responseLine == null)
					break;
				System.out.println("echo: " + responseLine);

				/*
				  The typical insert into orders table captured from the SOCKET looks like

                     BEGIN 959
                        table public.orders_stagging: INSERT: order_id[integer]:12 delivery_date[date]:'2017-07-18' user_id[integer]:1 zipcode[integer]:931 total[real]:11 item_count[integer]:1
                     COMMIT 959
				 
				 */
							
				
				// Validation of INSERT transaction on orders TABLE
				if (responseLine.startsWith("table public.orders_stagging: INSERT:")) {

					parseLine = responseLine.replace("table public.orders_stagging: INSERT:", "").trim().split(" ");

					// Extracting ZIPCODE field from the message

					 zipcode = parseLine[3].split(":")[1];
					
								 
				}

				
				 /*Publishing only zip codes which are valid as zip codes < length 5 will be errored out by zipcodeapi.com 
				 Normally the valid length is 5 but if you enter zip code with length 5 it will take first 5 digits.
				 Sometimes user by mistake types extra digit so, we should not skip such orders hence keeping this check for > = 5
				  */
				if(zipcode.length() >= 5) {
				
					// Sending ZIPCODE to the consumer instead of whole record to avoid data transfer over the network
				producer.send(new ProducerRecord<String, String>(topic, zipcode));
				
				}

			}

			// Closing Input Stream and Socket
			is.close();
			echoSocket.close();

			// Closing the Producer
			producer.close();

		} catch (Exception e) {
			System.err.println("Exception:  " + e);
		}
	}

	public static void main(String[] args) throws Exception {
		OrdersNewInsertProducer kProducer = new OrdersNewInsertProducer();

		// Publish message
		kProducer.PublishRecords();

	}

}
