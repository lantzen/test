package se.replyto.test.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;

import java.util.Properties;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.EndpointInject;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaClientFactory;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class SimplifiedKafkaProducerInTransactionWithLoopTest extends CamelTestSupport {
	@EndpointInject("mock:done")
	protected MockEndpoint doneEndpoint;
	
	private MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
			
	@Override
	protected CamelContext createCamelContext() throws Exception {
		CamelContext context = super.createCamelContext();
		
		KafkaClientFactory kcf = Mockito.mock(KafkaClientFactory.class);
		Mockito.when(kcf.getProducer(any(Properties.class))).thenReturn(mockProducer);
		
		KafkaComponent kafka = new KafkaComponent(); 
        kafka.getConfiguration().setBrokers("broker1:1234,broker2:4567");
        kafka.getConfiguration().setRecordMetadata(true);
        kafka.setKafkaClientFactory(kcf);
        		
		context.addComponent("kafka", kafka);
		
		return context;
	}
	
	/**
	 * In a Loop EIP creates Avro messages and sends them with transactional.id to Kafka. 
	 * The tests first shows that an IllegalStateException is caught in the route 
	 * before static property "CheckIfTransactedBy" on an updated version of 
	 * org.apache.camel.component.kafka.KafkaProducer.
	 */
	@Test
	public void test01_HappyLoopPath() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("*******************************").append(System.lineSeparator()) //
				.append("* Testing HappyPath with Loop *").append(System.lineSeparator()) //
				.append("*******************************").append(System.lineSeparator()));
		
		Exception exceptionCaught = null; 
		int messageCount = 5;
		int commitCount = 1;
		
		doneEndpoint.expectedMessageCount(1);

		try {
			template.sendBody("direct:split", messageCount);			
		} catch (CamelExecutionException e) {
			exceptionCaught = e;
		}
		
		assertInstanceOf(IllegalStateException.class, exceptionCaught.getCause());
		assertEquals("Transaction already started", exceptionCaught.getCause().getMessage());
		assertEquals(0, mockProducer.history().size());
		assertEquals(0, mockProducer.commitCount());

		org.apache.camel.component.kafka.KafkaProducer.setCheckIfTransactedBy(true);
		
		template.sendBody("direct:loop", messageCount);

		MockEndpoint.assertIsSatisfied(context);

		assertEquals(messageCount, mockProducer.history().size());
		assertEquals(commitCount, mockProducer.commitCount());
	}

	/**
	 * In the same route as test01_HappyLoopPath will throw a RuntimeException 
	 * to mark the mark exchange for RollbackOnly so no messages sent to Kafka are 
	 * received by clients that are configured with ISOLATION_LEVEL 'read_committed'
	 */
	@Test
	public void test02_OnExceptionWithLoop() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("*********************************").append(System.lineSeparator()) //
				.append("* Testing OnException with Loop *").append(System.lineSeparator()) //
				.append("*********************************").append(System.lineSeparator()));

		Exception exceptionCaught = null; 
		int throwExeptionOnIndex = 4;
		boolean checkIfTransactedBy = false;
		
		org.apache.camel.component.kafka.KafkaProducer.setCheckIfTransactedBy(checkIfTransactedBy);
		
		try {
			template.sendBodyAndHeader("direct:split", throwExeptionOnIndex+1, "ThrowExeptionOnIndex", throwExeptionOnIndex);			
		} catch (CamelExecutionException e) {
			exceptionCaught = e;
		}

		assertNotNull(exceptionCaught);
		
		if (checkIfTransactedBy) {
			assertInstanceOf(RuntimeException.class, exceptionCaught.getCause());
			assertEquals(exceptionCaught.getCause().getMessage(), "Failing with Index: "+throwExeptionOnIndex);
		} else {
			assertInstanceOf(IllegalStateException.class, exceptionCaught.getCause());
			assertEquals("Transaction already started", exceptionCaught.getCause().getMessage());
		}
		
		assertEquals(0, mockProducer.history().size());
		assertEquals(0, mockProducer.commitCount());
	}
	
	@Override
	protected RoutesBuilder createRouteBuilder() throws Exception {
				
		return new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				from("direct:loop")
					.id("loop")
					.setVariable("ThrowExeptionOnIndex", 
							header("ThrowExeptionOnIndex").convertTo(Integer.class))
					.loop(body().convertTo(Integer.class))
						.choice().when(exchange -> {
							Integer throwExeptionOnIndex = exchange.getVariable("ThrowExeptionOnIndex", Integer.class);
							Integer camelSplitIndex = exchange.getProperty("CamelLoopIndex", Integer.class);
							return (null != throwExeptionOnIndex && throwExeptionOnIndex == camelSplitIndex);
						})
							.throwException(RuntimeException.class, "Failing with Index: ${exchangeProperty.CamelLoopIndex}")
						.otherwise()
							.setBody(simple("test ${exchangeProperty.CamelLoopIndex}"))
							.to("kafka:loop?additional-properties[transactional.id]=1234&additional-properties[enable.idempotence]=true&additional-properties[retries]=5")
						.end() // .choice
					.end() // .loop
					.to("mock:done");

			}
		};
	}
}
