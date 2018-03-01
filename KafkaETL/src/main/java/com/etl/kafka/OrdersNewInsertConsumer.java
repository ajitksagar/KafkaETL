package com.etl.kafka;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * This consumer class is written to fetch the zipcodes from producer and load
 * the order_location_info_stagging table with (zipcode,city,state) after
 * getting the city and state info. for the respective zipcode from
 * zipcodeapi.com.
 * 
 * @author Ajit.Kshirsagar
 *
 */
public class OrdersNewInsertConsumer {

	private static KafkaConsumer<String, String> consumer;
	private static final String topic = "realtime_oders";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";

	/**
	 * getCityInfo() returns comma string with city name and state name
	 * 
	 * @param zipcode
	 * @return cityInfo
	 * @throws Exception
	 */

	public static String getCityInfo(String zipcode) throws Exception {

		String uri = "https://www.zipcodeapi.com/rest/lx7PkvTYlRycpK0XQu17HaPlwUw2xDQTbXIvsCtOt20TN1QEwSLd2Byzv4C3IsWT/info.json/"
				+ zipcode + "/degrees";
		String cityInfo = "";

		try {

			URL requestUrl = new URL(uri);
			// Creating HTTP Request from URL
			HttpURLConnection connection = (HttpURLConnection) requestUrl.openConnection();

			// Sending HTTP GET Request
			connection.setRequestMethod("GET");
			connection.setRequestProperty("Accept", "application/json");

			// Validating the HTTP Response
			if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
				throw new Exception("Failed : HTTP error code : " + connection.getResponseCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader((connection.getInputStream())));

			// Reading the JSON output
			String output = br.readLine();

			System.out.println("Output from Server .... \n");

			System.out.println(output);

			// Parsing the JSON output to extract city name and state
			JsonObject jsonObject = new JsonParser().parse(output).getAsJsonObject();

			String city = jsonObject.get("city").getAsString();
			String state = jsonObject.get("state").getAsString();

			cityInfo = city + "," + state;

			// Closing the connection
			connection.disconnect();

		} catch (MalformedURLException e) {

			e.printStackTrace();

		}

		return cityInfo;

	}

	/**
	 * createConsumer() returns created KafkaConsumer object to consume the
	 * message received from Producer
	 * 
	 * @return KafkaConsumer
	 */

	private static KafkaConsumer<String, String> createConsumer() {

		Properties props = new Properties();
		props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
		props.put("group.id", "grp");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		return new KafkaConsumer<String, String>(props);

	}

	/**
	 * consumeRecords() processes the zipcode received from Producer and stores
	 * the city info into order_location_info_stagging TABLE
	 * 
	 * @throws Exception
	 */

	public void consumeRecords() throws Exception {

		consumer = createConsumer();

		consumer.subscribe(Arrays.asList(topic));
		System.out.println("Subscribed to topic " + topic);

		// Creating JDBC Connection to PostgreSQL for record insertion
		String url = "jdbc:postgresql://localhost/postgres";
		Properties dbprops = new Properties();
		dbprops.setProperty("user", "postgres");
		dbprops.setProperty("password", "admin1234");
		Connection conn = DriverManager.getConnection(url, dbprops);
		Statement stmt = conn.createStatement();

		try {

			while (true) {

				ConsumerRecords<String, String> records = consumer.poll(100);

				for (ConsumerRecord<String, String> record : records) {

					System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(),
							record.value());

					// Extracting the order_id and zipcode from the message

					String zipcode = record.value();

					// Getting City and State Information from getCityInfo

					String cityInfo = getCityInfo(zipcode);

					String city = new String(cityInfo.split(",")[0]);
					String state = new String(cityInfo.split(",")[1]);

					// Insert query for inserting into order_location TABLE

					String insertQuery = "INSERT INTO order_location_info_stagging(zipcode,city,state) VALUES ('"
							+ zipcode + "','" + city + "','" + state + "')";

					// Executing the INSERT statement
					stmt.executeUpdate(insertQuery);

				}

			}

		} catch (SQLException e) {

			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws Exception {

		// Creating consumer object to call consumeRecords method
		OrdersNewInsertConsumer conSumer = new OrdersNewInsertConsumer();

		// Transforming the message and writing location information to
		// PostgreSQL table
		conSumer.consumeRecords();
	}

}
