package se.replyto.test.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;

import java.util.Properties;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaClientFactory;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mockito;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class MultipleKafkaProducerInTransactionTest extends CamelTestSupport {
	@EndpointInject("mock:done")
	protected MockEndpoint doneEndpoint;
	
	private MockProducer<String, String> mockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
			
    private Throwable exceptionCaught = null; 

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
	
	@Test
	public void test01_HappyPath() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("*********************").append(System.lineSeparator()) //
				.append("* Testing HappyPath *").append(System.lineSeparator()) //
				.append("*********************").append(System.lineSeparator()));
		
		exceptionCaught = null;
		
		try {
			testHappyPath("Hello World!", false);
		} catch (CamelExecutionException e) {
			System.err.printf("Unexpected %s thrown by route with message: %s", e.getClass().getSimpleName(), e.getMessage());
		}
		
		assertNull(exceptionCaught, String.format("Unexpected %s caught in route with message: %s", exceptionCaught.getClass().getSimpleName(), exceptionCaught.getMessage()));
	}
	
	@Test
	public void test02_HappyPathWithSingleKafkaProcucer() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("***********************************************").append(System.lineSeparator()) //
				.append("* Testing HappyPath with single KafkaProcucer *").append(System.lineSeparator()) //
				.append("***********************************************").append(System.lineSeparator()));
		
		testHappyPath("topic1:test1;topic2:test2", true);
	}

	private void testHappyPath(String body, boolean useSingleKafkaProcucer) throws InterruptedException {
		doneEndpoint.setExpectedCount(1);

		template.sendBodyAndHeader("direct:start", body, "UseSingleKafkaProcucer", useSingleKafkaProcucer);

		MockEndpoint.assertIsSatisfied(context);

		assertEquals(2, mockProducer.history().size());
		ProducerRecord<String, String> record1 = mockProducer.history().get(0);
		assertEquals("topic1", record1.topic());
		assertEquals("test1", record1.value());
		ProducerRecord<String, String> record2 = mockProducer.history().get(1);
		assertEquals("topic2", record2.topic());
		assertEquals("test2", record2.value());
		assertEquals(1, mockProducer.commitCount());
	}
	
	@Override
	protected RoutesBuilder createRouteBuilder() throws Exception {
				
		return new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				from("direct:start")
					.doTry()
						.choice()
							.when(header("UseSingleKafkaProcucer"))
								.split(body().tokenize(";")).shareUnitOfWork(true).stopOnException()
									.setBody(exchange -> {
										Message message = exchange.getMessage();
										String body = message.getBody(String.class);
										String[] split = body.split(":");
										
										message.removeHeaders(".*");
										message.setHeader(KafkaConstants.OVERRIDE_TOPIC, split[0]);
										return split[1];
									})
									.toD("kafka:topic?additional-properties[transactional.id]=1234&additional-properties[enable.idempotence]=true&additional-properties[retries]=5")
								.end() // .split
							.endChoice()
							.otherwise()
								.setBody(constant("test1"))
								.toD("kafka:topic1?additional-properties[transactional.id]=1234&additional-properties[enable.idempotence]=true&additional-properties[retries]=5")
								.setBody(constant("test2"))
								.toD("kafka:topic2?additional-properties[transactional.id]=1234&additional-properties[enable.idempotence]=true&additional-properties[retries]=5")
						.end() // .choice()
					.endDoTry()
					.doCatch(Throwable.class)
						.process(exchange -> {
							exceptionCaught = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
							exceptionCaught.printStackTrace();
						})
						.markRollbackOnly()
					.end() // .doTry()
					.to("mock:done");

			}
		};
	}
}
