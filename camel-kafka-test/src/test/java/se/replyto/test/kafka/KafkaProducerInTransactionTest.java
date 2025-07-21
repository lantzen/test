package se.replyto.test.kafka;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.camel.CamelContext;
import org.apache.camel.CamelExchangeException;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.component.ComponentsBuilderFactory;
import org.apache.camel.builder.component.dsl.KafkaComponentBuilderFactory.KafkaComponentBuilder;
import org.apache.camel.component.kafka.KafkaProducer;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.spi.UnitOfWork;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.ziklo.infobus.camel.example.kafka.TestMessage;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class KafkaProducerInTransactionTest extends CamelTestSupport {
	private static final String TOPIC = "integration.test.kafka.example";

	private static Logger logger = LogManager.getLogger();

	@EndpointInject("mock:done")
	protected MockEndpoint doneEndpoint;

	@EndpointInject("mock:exception")
	protected MockEndpoint exceptionEndpoint;

	@Produce("direct:start")
	protected ProducerTemplate template;

	private static AdminClient adminClient = null;
	private static Map<String, String> kafkaConfig;
	private static Properties producerConfig = null;
	private static Properties consumerConfig = null;
	private static String schemaRegistryConfig = null;
	private static KafkaComponentBuilder kafkaComponentBuilder = null;

	private Throwable exceptionCaughtByRoute = null;

	protected static Map<String, String> createKafkaConfig(String kafkaUrl, String schemaRegistryUrl,
			String securityProtocol, String schemaRegistryCredentialsSource, String schemaRegistryAuthInfo,
			String sslEndpointAlgorithm, String truststorePath, String truststorePassword, String keystorePath,
			String keystorePassword, String keystoreType, String keyPassword) {
		kafkaConfig = new HashMap<>();
		kafkaConfig.put("bootstrap.servers", kafkaUrl); // ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
		kafkaConfig.put("schema.registry.url", schemaRegistryUrl);

		kafkaConfig.put("security.protocol", securityProtocol); // CommonClientConfigs.SECURITY_PROTOCOL_CONFIG
		kafkaConfig.put("ssl.truststore.location", truststorePath); // SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG
		kafkaConfig.put("ssl.truststore.password", truststorePassword); // SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG

		// configure the following three settings for SSL Authentication
		kafkaConfig.put("ssl.keystore.type", keystoreType); // SslConfigs.SSL_KEYSTORE_TYPE_CONFIG
		kafkaConfig.put("ssl.keystore.location", keystorePath); // SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG
		kafkaConfig.put("ssl.keystore.password", keystorePassword); // SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG
		kafkaConfig.put("ssl.key.password", keyPassword); // SslConfigs.SSL_KEY_PASSWORD_CONFIG

		kafkaConfig.put("basic.auth.credentials.source", schemaRegistryCredentialsSource); // URL, USER_INFO and
																							// SASL_INHERIT
		kafkaConfig.put("basic.auth.user.info", schemaRegistryAuthInfo);// {username}:{password}
		kafkaConfig.put("schema.registry.ssl.truststore.location", truststorePath);
		kafkaConfig.put("schema.registry.ssl.truststore.password", truststorePassword);
		kafkaConfig.put("schema.registry.ssl.keystore.location", keystorePath);
		kafkaConfig.put("schema.registry.ssl.keystore.password", keystorePassword);
		kafkaConfig.put("schema.registry.ssl.keystore.type", keystoreType);
		kafkaConfig.put("schema.registry.ssl.key.password", keyPassword);

		kafkaConfig.put("ssl.endpoint.identification.algorithm", sslEndpointAlgorithm);

		logger.info("Configuring kafka.brokers={} with keystore={}, truststore={}", kafkaUrl, keystorePath,
				truststorePath);

		return Collections.unmodifiableMap(kafkaConfig);
	}

	private static class KafkaTestConsumer implements Runnable {
		AtomicInteger totalPoisonMessageSkipped = new AtomicInteger(0);
		AtomicInteger totalMessageConsumed = new AtomicInteger(0);

		final Duration pollTimeout;
		final String isolationLevel;

		public KafkaTestConsumer() {
			this(Duration.ofSeconds(5), "read_committed");
		}

		public KafkaTestConsumer(Duration pollTimeout, String isolationLevel) {
			this.pollTimeout = pollTimeout;
			this.isolationLevel = isolationLevel;
		}

		public int getTotalPoisonMessageSkipped() {
			return totalPoisonMessageSkipped.get();
		}

		public int getTotalMessageConsumed() {
			return totalMessageConsumed.get();
		}

		public int getTotalMessage() {
			return getTotalPoisonMessageSkipped() + getTotalMessageConsumed();
		}

		@Override
		public void run() {
			Properties testConsumerConfig = new Properties();

			testConsumerConfig.putAll(consumerConfig);
			testConsumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);

			try (KafkaConsumer<String, Record> consumer = new KafkaConsumer<>(testConsumerConfig)) {
				TopicPartition poisonPillPartition = null;
				long poisonPillOffset = -1;

				consumer.subscribe(Arrays.asList(TOPIC));

				while (true) {
					if (null != poisonPillPartition && -1 != poisonPillOffset) {
						System.err.printf(
								"Skipping 'poison pill' Kafka message with offest %d at partition %d of topic %s%n",
								poisonPillOffset, poisonPillPartition.partition(), poisonPillPartition.topic());

						consumer.seek(poisonPillPartition, poisonPillOffset + 1);

						totalPoisonMessageSkipped.incrementAndGet();

						poisonPillPartition = null;
						poisonPillOffset = -1;
					}

					try {
						long receiveStartMillis = System.currentTimeMillis();
						ConsumerRecords<String, Record> records = consumer.poll(pollTimeout);
						System.err.printf("*** Received %d message after %d ms%n", records.count(),
								System.currentTimeMillis() - receiveStartMillis);

						if (0 == records.count()) {
							break;
						}
						Iterator<ConsumerRecord<String, Record>> i = records.iterator();
						while (i.hasNext()) {
							ConsumerRecord<java.lang.String, Record> consumerRecord = i.next();

							Object value = consumerRecord.value();
							if (value instanceof org.apache.avro.generic.GenericData.Record) {
								Schema schema = ((org.apache.avro.generic.GenericData.Record) value).getSchema();
								System.out.printf("Value '%s' with schema '%s'%n", value, schema);
							} else {
								System.out.printf("Value '%s'%n", value);
							}

							totalMessageConsumed.incrementAndGet();
						}
					} catch (RecordDeserializationException e) {
						poisonPillPartition = e.topicPartition();
						poisonPillOffset = e.offset();
					}
				}
			}
		}

	}

	@BeforeAll
	public static void setup() {
		String securityProtocol = "SSL";
		String schemaRegistryCredentialsSource = "USER_INFO";
		String kafkaUrl = System.getenv("KAFKA_URL");
		String kafkaSchemaRegistryUrl = System.getenv("KAFKA_SCHEMA_REGISTRY_URL");
		String kafkaSchemaRegistryUsername = System.getenv("KAFKA_SCHEMA_REGISTRY_USERNAME");
		String kafkaSchemaRegistryPassword = System.getenv("KAFKA_SCHEMA_REGISTRY_PASSWORD");
		String schemaRegistryAuthInfo = kafkaSchemaRegistryUsername + ":" + kafkaSchemaRegistryPassword;
		String sslEndpointAlgorithm = ""; // Disable hostname verification
		String truststorePath = System.getenv("TRUSTSTORE_FILE");
		String truststorePassword = "";
		String keystorePath = System.getenv("KEYSTORE_FILE");
		String keystorePassword = System.getenv("KEYSTORE_PASSWORD");
		String keystoreType = "PKCS12";
		String keyPassword = keystorePassword;

		kafkaConfig = createKafkaConfig(kafkaUrl, kafkaSchemaRegistryUrl, securityProtocol,
				schemaRegistryCredentialsSource, schemaRegistryAuthInfo, sslEndpointAlgorithm, truststorePath,
				truststorePassword, keystorePath, keystorePassword, keystoreType, keyPassword);

		producerConfig = new Properties();

		producerConfig.putAll(kafkaConfig);

		// Producer specific config
		producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
		producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
		producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());// "org.apache.kafka.common.serialization.StringSerializer");
		producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());// "org.apache.kafka.common.serialization.StringSerializer");

		consumerConfig = new Properties();

		consumerConfig.putAll(kafkaConfig);

		// Add consumer specific config
		consumerConfig.putAll(kafkaConfig);
		consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "integration.unit-test.01");
		consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());// "org.apache.kafka.common.serialization.StringDeserializer");
		consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
		consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		schemaRegistryConfig = new StringBuilder().append("&additional-properties[schema.registry.url]=")
				.append(kafkaSchemaRegistryUrl).append("&additional-properties[basic.auth.credentials.source]=")
				.append(schemaRegistryCredentialsSource).append("&additional-properties[basic.auth.user.info]=")
				.append(schemaRegistryAuthInfo).append("&additional-properties[schema.registry.ssl.keystore.location]=")
				.append(keystorePath).append("&additional-properties[schema.registry.ssl.keystore.password]=")
				.append(keystorePassword).append("&additional-properties[schema.registry.ssl.key.password]=")
				.append(keyPassword).append("&additional-properties[schema.registry.ssl.truststore.location]=")
				.append(truststorePath).append("&additional-properties[schema.registry.ssl.truststore.password]=")
				.append(truststorePassword).toString();

		kafkaComponentBuilder = ComponentsBuilderFactory.kafka().brokers(kafkaUrl);
		kafkaComponentBuilder.schemaRegistryURL(kafkaSchemaRegistryUrl);
		kafkaComponentBuilder.securityProtocol(securityProtocol)
				// Configure the following three settings for SSL Encryption
				.sslTruststoreLocation(truststorePath).sslTruststorePassword(truststorePassword);
		kafkaComponentBuilder.sslKeystoreType(keystoreType)
				// Configure the following three settings for SSL Authentication
				.sslKeystoreLocation(keystorePath).sslKeystorePassword(keystorePassword).sslKeyPassword(keyPassword);

		Map<String, Object> additionalProperties = new HashMap<>();
		additionalProperties.put("schema.registry.url", kafkaSchemaRegistryUrl);
		additionalProperties.put("basic.auth.credentials.source", schemaRegistryCredentialsSource);
		additionalProperties.put("basic.auth.user.info", schemaRegistryAuthInfo);

		kafkaComponentBuilder.additionalProperties(additionalProperties);
		kafkaComponentBuilder.sslEndpointAlgorithm(sslEndpointAlgorithm);
	}

	@Test
	public void test01_HappyLoopPath() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("*******************************").append(System.lineSeparator()) //
				.append("* Testing HappyPath with Loop *").append(System.lineSeparator()) //
				.append("*******************************").append(System.lineSeparator()));

		KafkaTestConsumer kafkaTestConsumer = new KafkaTestConsumer();
		Thread kafkaTestConsumerThread = new Thread(kafkaTestConsumer, "Kafka TestConsumer 1");
		int messageCount = 5;

		exceptionCaughtByRoute = null;
		exceptionEndpoint.expectedMessageCount(1);
		KafkaProducer.setCheckIfTransactedBy(false);

		// Remove messages from old tests
		kafkaTestConsumer.run();

		kafkaTestConsumerThread.start();

		template.sendBody("direct:loop", messageCount);

		MockEndpoint.assertIsSatisfied(context);

		assertInstanceOf(IllegalStateException.class, exceptionCaughtByRoute);
		assertThat(exceptionCaughtByRoute.getMessage(),
				containsString("Invalid transition attempted from state IN_TRANSACTION to state IN_TRANSACTION"));

		kafkaTestConsumerThread.join();
		assertEquals(0, kafkaTestConsumer.getTotalMessage(),
				"Unspected total message count: " + kafkaTestConsumer.getTotalMessage());
		kafkaTestConsumerThread = new Thread(kafkaTestConsumer, "Kafka TestConsumer 2");
		kafkaTestConsumerThread.start();

		exceptionCaughtByRoute = null;
		doneEndpoint.expectedMessageCount(1);
		KafkaProducer.setCheckIfTransactedBy(true);

		template.sendBody("direct:loop", messageCount);

		MockEndpoint.assertIsSatisfied(context);

		assertNull(exceptionCaughtByRoute);

		kafkaTestConsumerThread.join();

		assertEquals(messageCount, kafkaTestConsumer.getTotalMessage(),
				"Unspected total message count: " + kafkaTestConsumer.getTotalMessage());
		assertEquals(messageCount, kafkaTestConsumer.getTotalMessageConsumed(),
				"Unspected consumed message count: " + kafkaTestConsumer.getTotalMessageConsumed());
	}

	@Test
	public void test02_OnExceptionWithLoop() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("*********************************").append(System.lineSeparator()) //
				.append("* Testing OnException with Loop *").append(System.lineSeparator()) //
				.append("*********************************").append(System.lineSeparator()));

		KafkaTestConsumer kafkaTestConsumer = new KafkaTestConsumer();
		Thread kafkaTestConsumerThread = new Thread(kafkaTestConsumer, "Kafka TestConsumer");

		exceptionEndpoint.expectedMessageCount(1);
		exceptionCaughtByRoute = null;
		KafkaProducer.setCheckIfTransactedBy(true);

		// Remove messages from old tests
		kafkaTestConsumer.run();

		kafkaTestConsumerThread.start();

		template.sendBodyAndHeader("direct:loop", 2, "ThrowExeptionOnLoopIndex", 1);

		MockEndpoint.assertIsSatisfied(context);

		assertInstanceOf(RuntimeException.class, exceptionCaughtByRoute);
		assertEquals(exceptionCaughtByRoute.getMessage(), "Failing with camelLoopIndex: 1");

		kafkaTestConsumerThread.join();

		assertEquals(0, kafkaTestConsumer.getTotalMessage(),
				"Unspected total message count: " + kafkaTestConsumer.getTotalMessage());
	}

	@Test
	public void test03_HappySplitPath() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("********************************").append(System.lineSeparator()) //
				.append("* Testing HappyPath with Split *").append(System.lineSeparator()) //
				.append("********************************").append(System.lineSeparator()));

		KafkaTestConsumer kafkaTestConsumer = new KafkaTestConsumer();
		Thread kafkaTestConsumerThread = new Thread(kafkaTestConsumer, "Kafka TestConsumer");
		StringBuilder sb = new StringBuilder();
		int messageCount = 5;

		for (int i = 0; i < messageCount; i++) {
			sb.append(String.format("test%02d\n", i + 1));
		}

		// Remove messages from old tests
		kafkaTestConsumer.run();

		kafkaTestConsumerThread.start();

		doneEndpoint.expectedMessageCount(1);

		exceptionCaughtByRoute = null;
		KafkaProducer.setCheckIfTransactedBy(false);

		template.sendBody("direct:split", sb.toString());

		MockEndpoint.assertIsSatisfied(context, 10, TimeUnit.SECONDS);

		assertNull(exceptionCaughtByRoute);

		kafkaTestConsumerThread.join();

		assertEquals(messageCount, kafkaTestConsumer.getTotalMessage(),
				"Unspected total message count: " + kafkaTestConsumer.getTotalMessage());

	}

	@Test
	public void test04_OnExceptionWithSplit() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("**********************************").append(System.lineSeparator()) //
				.append("* Testing OnException with Split *").append(System.lineSeparator()) //
				.append("**********************************").append(System.lineSeparator()));

		KafkaTestConsumer kafkaTestConsumer = new KafkaTestConsumer();
		Thread kafkaTestConsumerThread = new Thread(kafkaTestConsumer, "Kafka TestConsumer");
		StringBuilder sb = new StringBuilder();
		int messageCount = 2;

		for (int i = 0; i < messageCount; i++) {
			sb.append(String.format("test%02d\n", i + 1));
		}

		exceptionEndpoint.expectedMessageCount(1);
		exceptionCaughtByRoute = null;
		KafkaProducer.setCheckIfTransactedBy(true);

		// Remove messages from old tests
		kafkaTestConsumer.run();

		kafkaTestConsumerThread.start();

		template.sendBodyAndHeader("direct:split", sb.toString(), "ThrowExeptionOnSplitIndex", 1);

		MockEndpoint.assertIsSatisfied(context);

		assertInstanceOf(CamelExchangeException.class, exceptionCaughtByRoute);
		assertInstanceOf(RuntimeException.class, exceptionCaughtByRoute.getCause());
		assertEquals(exceptionCaughtByRoute.getCause().getMessage(), "Failing with camelSplitIndex: 1");

		kafkaTestConsumerThread.join();

		assertEquals(0, kafkaTestConsumer.getTotalMessage(),
				"Unspected total message count: " + kafkaTestConsumer.getTotalMessage());
	}

	@Override
	protected CamelContext createCamelContext() throws Exception {
		CamelContext context = super.createCamelContext();

		context.addComponent("kafka", kafkaComponentBuilder.build());
		return context;
	}

	@Override
	protected RoutesBuilder createRouteBuilder() throws Exception {
		return new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				String loopDestinationUrl = "kafka:" + TOPIC
						+ "?keySerializer=org.apache.kafka.common.serialization.StringSerializer"
						+ "&valueSerializer=io.confluent.kafka.serializers.KafkaAvroSerializer" + schemaRegistryConfig;
				String splitDestinationUrl = loopDestinationUrl
						+ "&additional-properties[transactional.id]=1234&additional-properties[enable.idempotence]=true&additional-properties[retries]=5";

				loopDestinationUrl += "&additional-properties[transactional.id]=5678&additional-properties[enable.idempotence]=true&additional-properties[retries]=5";

				from("direct:loop").id(KafkaProducerInTransactionTest.class.getName() + "-00_test_loop")
						.setVariable("MessageCount", body().convertTo(Integer.class))
						.setVariable("ThrowExeptionOnLoopIndex",
								header("ThrowExeptionOnLoopIndex").convertTo(Integer.class))
						.doTry().loop(variable("MessageCount")).setBody(exchange -> {
							Integer throwExeptionOnLoopIndex = exchange.getVariable("ThrowExeptionOnLoopIndex", Integer.class);
							Integer camelLoopIndex = exchange.getProperty("CamelLoopIndex", Integer.class);
							UnitOfWork uow = exchange.getUnitOfWork();

							if (null != throwExeptionOnLoopIndex && throwExeptionOnLoopIndex == camelLoopIndex) {
								System.err.println("Failing with exception at camelLoopIndex: " + camelLoopIndex);
								throw new RuntimeException("Failing with camelLoopIndex: " + camelLoopIndex);
							}

							if (camelLoopIndex > 0 && !uow.isTransacted()) {
								System.err.println("***** UnitOfWork is not transacted on camelLoopIndex: " + camelLoopIndex);
							}

							System.out.printf("***** Creating TestMessage in Loop exchange with id '%s' and UnitOfWork: %s%n",
									exchange.getExchangeId(), exchange.getUnitOfWork().hashCode());

							return new TestMessage(String.format("test", camelLoopIndex + 1));
						}).to(loopDestinationUrl).end() // .loop
						.endDoTry().doCatch(Throwable.class).to("mock:exception").process(exchange -> {
							exceptionCaughtByRoute = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
						}).markRollbackOnly().end() // .doCatch
						.to("mock:done");

				from("direct:split").id(KafkaProducerInTransactionTest.class.getName() + "-00_test_split")
						.setVariable("ThrowExeptionOnSplitIndex", 
								header("ThrowExeptionOnSplitIndex").convertTo(Integer.class))
						.doTry().split(body().tokenize("\n")).shareUnitOfWork(true).stopOnException()
						.setBody(exchange -> {
							Integer throwExeptionOnSplitIndex = exchange.getVariable("ThrowExeptionOnSplitIndex", Integer.class);
							Integer camelSplitIndex = exchange.getProperty("CamelSplitIndex", Integer.class);
							String body = exchange.getMessage().getBody(String.class);
							UnitOfWork uow = exchange.getUnitOfWork();

							if (null != throwExeptionOnSplitIndex && throwExeptionOnSplitIndex == camelSplitIndex) {
								System.err.println("Failing with exception at camelLoopIndex: " + camelSplitIndex);
								throw new RuntimeException("Failing with camelSplitIndex: " + camelSplitIndex);
							}

							System.out.printf(
									"***** Creating TestMessage in Split exchange with id '%s' and UnitOfWork: %s%n",
									exchange.getExchangeId(), uow.hashCode());

							return new TestMessage(String.format("test%02d - %s", camelSplitIndex + 1, body));
						}).to(splitDestinationUrl).end() // .split rows
						.endDoTry().doCatch(Throwable.class).to("mock:exception").process(exchange -> {
							exceptionCaughtByRoute = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
							exceptionCaughtByRoute.printStackTrace();
						}).markRollbackOnly().end() // .doCatch
						.to("mock:done");
			}
		};
	}
}