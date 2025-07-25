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
public class MultipleKafkaProducerInTransactionTest extends CamelTestSupport {
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
	
	@Test
	public void test01_HappyPath() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("*********************").append(System.lineSeparator()) //
				.append("* Testing HappyPath *").append(System.lineSeparator()) //
				.append("*********************").append(System.lineSeparator()));
		
		doneEndpoint.setExpectedCount(1);

		template.sendBody("direct:start", "Hello World!");

		MockEndpoint.assertIsSatisfied(context);

		assertEquals(2, mockProducer.history().size());
		assertEquals(1, mockProducer.commitCount());
	}
	
	@Override
	protected RoutesBuilder createRouteBuilder() throws Exception {
				
		return new RouteBuilder() {
			@Override
			public void configure() throws Exception {
				from("direct:start")
					.setBody(simple("test1 ${exchangeId}"))
					.to("kafka:topic1?additional-properties[transactional.id]=1234&additional-properties[enable.idempotence]=true&additional-properties[retries]=5")
					.setBody(simple("test2 ${exchangeId}"))
					.to("kafka:topic2?additional-properties[transactional.id]=1234&additional-properties[enable.idempotence]=true&additional-properties[retries]=5")
					.to("mock:done");

			}
		};
	}
}
