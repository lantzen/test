package se.replyto.test.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;

import java.util.Properties;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExchangeException;
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
public class SimplifiedKafkaProducerInTransactionTest extends CamelTestSupport {
	@EndpointInject("mock:loop-done")
	protected MockEndpoint loopDoneEndpoint;
	
	@EndpointInject("mock:split-done")
	protected MockEndpoint splitDoneEndpoint;
	
	private MockProducer<String, String> loopMockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
	private MockProducer<String, String> splitMockProducer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());// {
//		public void beginTransaction() throws org.apache.kafka.common.errors.ProducerFencedException {
//			super.beginTransaction();
//		};
//	};
	
	@Override
	protected CamelContext createCamelContext() throws Exception {
		CamelContext context = super.createCamelContext();
		
		KafkaClientFactory kcf = Mockito.mock(KafkaClientFactory.class);
//		Mockito.when(kcf.getProducer(any(Properties.class))).thenReturn(mockProducer);
		Mockito.when(kcf.getProducer(any(Properties.class))).thenAnswer(invocation -> {
			Properties kafkaProps = invocation.getArgument(0);
			Object transactionalId = kafkaProps.get("transactional.id");
			if ("1234".equals(transactionalId)) {
				return loopMockProducer;				
			} else {
				return splitMockProducer;
			}
		});
		
        KafkaComponent kafka = new KafkaComponent(context);
        kafka.getConfiguration().setBrokers("broker1:1234,broker2:4567");
        kafka.getConfiguration().setRecordMetadata(true);
        kafka.setKafkaClientFactory(kcf);
//        kafka.init();

		context.addComponent("kafka", kafka);
		return context;
	}
	
	@Test
	public void test01_HappyLoopPath() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("*******************************").append(System.lineSeparator()) //
				.append("* Testing HappyPath with Loop *").append(System.lineSeparator()) //
				.append("*******************************").append(System.lineSeparator()));
		
		Exception exceptionCaught = null; 
		int messageCount = 5;
		int commitCount = 1;
		
		splitDoneEndpoint.expectedMessageCount(commitCount);

//		org.apache.camel.component.kafka.KafkaProducer.setCheckIfTransactedBy(true);
		try {
			template.sendBody("direct:loop", messageCount);			
		} catch (CamelExecutionException e) {
			exceptionCaught = e;
		}
		
		if (null != exceptionCaught) {
			assertInstanceOf(IllegalStateException.class, exceptionCaught.getCause());
			assertEquals("Transaction already started", exceptionCaught.getCause().getMessage());
			messageCount = 0;
			commitCount = 0;
		}
		
		assertEquals(messageCount, loopMockProducer.history().size());
		assertEquals(commitCount, loopMockProducer.commitCount());
	}

	@Test
	public void test02_OnExceptionWithLoop() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("*********************************").append(System.lineSeparator()) //
				.append("* Testing OnException with Loop *").append(System.lineSeparator()) //
				.append("*********************************").append(System.lineSeparator()));

		Exception exceptionCaught = null; 
		int throwExeptionOnIndex = 4;

//		org.apache.camel.component.kafka.KafkaProducer.setCheckIfTransactedBy(true);
		
		try {
			template.sendBodyAndHeader("direct:loop", throwExeptionOnIndex+1, "ThrowExeptionOnIndex", throwExeptionOnIndex);			
		} catch (CamelExecutionException e) {
			exceptionCaught = e;
		}

		assertNotNull(exceptionCaught);
		
		if (exceptionCaught.getCause() instanceof IllegalStateException) {
			assertEquals("Transaction already started", exceptionCaught.getCause().getMessage());
		} else {
			assertInstanceOf(RuntimeException.class, exceptionCaught.getCause());
			assertEquals(exceptionCaught.getCause().getMessage(), "Failing with Index: "+throwExeptionOnIndex);
		}
		
		assertEquals(0, loopMockProducer.history().size());
		assertEquals(0, loopMockProducer.commitCount());
	}
	
	@Test
	public void test03_HappySplitPath() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("********************************").append(System.lineSeparator()) //
				.append("* Testing HappyPath with Split *").append(System.lineSeparator()) //
				.append("********************************").append(System.lineSeparator()));

		StringBuilder sb = new StringBuilder();
		int messageCount = 5;

		
		splitDoneEndpoint.expectedMessageCount(1);

		for (int i = 0; i < messageCount; i++) {
			sb.append(String.format("test%02d\n", i + 1));
		}
		
		template.sendBody("direct:split", sb.toString());			
		
		assertEquals(messageCount, splitMockProducer.history().size());
		assertEquals(1, splitMockProducer.commitCount());
	}

	@Test
	public void test04_OnExceptionWithSplit() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("**********************************").append(System.lineSeparator()) //
				.append("* Testing OnException with Split *").append(System.lineSeparator()) //
				.append("**********************************").append(System.lineSeparator()));

		StringBuilder sb = new StringBuilder();
		Exception exceptionCaught = null;
		int throwExeptionOnIndex = 4;
		
		for (int i = 0; i < throwExeptionOnIndex+1; i++) {
			sb.append(String.format("test%02d\n", i + 1));
		}
		
		try {
			template.sendBodyAndHeader("direct:split", sb.toString(), "ThrowExeptionOnIndex", throwExeptionOnIndex);			
		} catch (CamelExecutionException e) {
			exceptionCaught = e;
		}

		assertInstanceOf(CamelExchangeException.class, exceptionCaught.getCause());
		assertInstanceOf(RuntimeException.class, exceptionCaught.getCause().getCause());
		assertEquals("Failing with Index: "+throwExeptionOnIndex, exceptionCaught.getCause().getCause().getMessage());

		assertEquals(0, splitMockProducer.history().size());
		assertEquals(0, splitMockProducer.commitCount());
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
					.to("mock:loop-done");

				from("direct:split")
					.id("split")
					.setVariable("ThrowExeptionOnIndex", 
							header("ThrowExeptionOnIndex").convertTo(Integer.class))
					.split(body().tokenize("\n")).shareUnitOfWork(true).stopOnException()
						.choice().when(exchange -> {
							Integer throwExeptionOnIndex = exchange.getVariable("ThrowExeptionOnIndex", Integer.class);
							Integer camelSplitIndex = exchange.getProperty("CamelSplitIndex", Integer.class);
							return (null != throwExeptionOnIndex && throwExeptionOnIndex == camelSplitIndex);
						})
							.throwException(RuntimeException.class, "Failing with Index: ${exchangeProperty.CamelSplitIndex}")
						.otherwise()
							.to("kafka:split?additional-properties[transactional.id]=45678&additional-properties[enable.idempotence]=true&additional-properties[retries]=5")
						.end() // .choice
					.end() // .split
					.to("mock:split-done");
			}
		};
	}
}
