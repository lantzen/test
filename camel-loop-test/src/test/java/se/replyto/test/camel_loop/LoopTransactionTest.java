package se.replyto.test.camel_loop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExchangeException;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RollbackExchangeException;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.component.ComponentsBuilderFactory;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.spring.spi.SpringTransactionPolicy;
import org.apache.camel.test.junit5.CamelTestSupport;
import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

@TestMethodOrder(MethodOrderer.MethodName.class)
public class LoopTransactionTest extends CamelTestSupport {
	private final static int BATCH_CNT = 10;
	private final static int BATCH_SIZE = 10000;

	private static Logger logger = LogManager.getLogger();
	
    @EndpointInject("mock:done")
    protected MockEndpoint doneEndpoint;

    @EndpointInject("mock:exception")
    protected MockEndpoint exceptionEndpoint;

    @Produce("direct:start")
    protected ProducerTemplate template;

	private static EmbeddedDataSource testDataSource;

    @BeforeAll
    public static void setup() {       	
		testDataSource = new EmbeddedDataSource(); 
		testDataSource.setDatabaseName("TestDb");
		testDataSource.setCreateDatabase("create");
		
		try (Connection testConnection = testDataSource.getConnection();
		    	Statement testStatement = testConnection.createStatement()) {
			
	    	try {
				testStatement.executeUpdate("DROP TABLE test");
			} catch (SQLException e) {
				if (!SQLState.LANG_OBJECT_DOES_NOT_EXIST.startsWith(e.getSQLState()))
					logger.warn("Failed to drop test table before recreating it", e);
			}

			testStatement.execute("CREATE TABLE test("
	    	         + "ROW_ID INTEGER NOT NULL GENERATED ALWAYS AS IDENTITY, "
	    	         + "TEST VARCHAR(40) NOT NULL, "
	    	         + "PRIMARY KEY (ROW_ID))");
			
		} catch (SQLException e) {
			String message = "Failed to create test database connection with test table";
			logger.warn(message, e);
			throw new RuntimeException(message, e);
		}
    }
    
    @Test
    public void test00_PlainJdbcInsert() throws Exception {
    	long memoryUsageBeforeLoadingData;

		try (Connection conn = testDataSource.getConnection();
		    	Statement testStatement = conn.createStatement()) {
			ResultSet rs = testStatement.executeQuery("SELECT count(1) from test");
			int recordCntAfter, recordCntBefore, rowCnt = BATCH_SIZE*BATCH_CNT;

			assertTrue(rs.next(), "Unable to count records in db before test");
			recordCntBefore = rs.getInt(1);

			conn.setAutoCommit(false);
			
			memoryUsageBeforeLoadingData = getCurrentlyUsedMemory();
			logger.info("Used memory before loading some data: " + memoryUsageBeforeLoadingData + " MB");

			try (PreparedStatement pStatement = conn.prepareStatement("INSERT INTO test(TEST) VALUES(?)")) {
				int batchNumber = 0;

				for (int i = 0; i < BATCH_CNT; i++) {
					int rowIdFrom = i*BATCH_SIZE+1;
					int rowIdTo = (i+1)*BATCH_SIZE;
					
					for (int rowId = rowIdFrom; rowId <= rowIdTo; rowId++) {
						pStatement.setString(1, String.format("test%-36s", rowId));
						pStatement.addBatch();
					}
					
					int[] updateCounts = pStatement.executeBatch();
					assertEquals(BATCH_SIZE, updateCounts.length);
					for (int updateCount : updateCounts) {
						assertEquals(1, updateCount);
					}
					
					long memoryUsageAfterLoadingData = getCurrentlyUsedMemory();
					System.out.printf("Executed batch %d with %d elements and %d MB memory used%n",
							++batchNumber, BATCH_SIZE,
							memoryUsageAfterLoadingData - memoryUsageBeforeLoadingData);
				}

				long memoryUsageAfterLoadingData = getCurrentlyUsedMemory();
				logger.info("Used memory after loading some data: " + memoryUsageAfterLoadingData + " MB");
				logger.info("Difference: " + (memoryUsageAfterLoadingData - memoryUsageBeforeLoadingData) + " MB");
			}
			conn.commit();
			
			conn.setAutoCommit(true);

			rs.close();
			rs = testStatement.executeQuery("SELECT count(1) from test");
			
			assertTrue(rs.next(), "Unable to count records in db after test");
			recordCntAfter = rs.getInt(1);			
			assertEquals(rowCnt, recordCntAfter-recordCntBefore, "Unexpected number of rows in db");

    		rs.close();
			rs = testStatement.executeQuery("SELECT * from test FETCH FIRST 10 ROWS ONLY");
			assertTrue(rs.next(), "Unable to show first 10 records in db after test");
			assertEquals(1, rs.getInt(1));
			assertEquals(String.format("test%-36s", 1), rs.getString(2));
			System.out.println("10 first records in test table:");
			do {
				System.out.printf("\t%2d: %s%n", rs.getInt(1), rs.getString(2));
			} while (rs.next());

    		rs.close();
			rs = testStatement.executeQuery("SELECT * from test ORDER BY ROW_ID DESC FETCH FIRST 10 ROWS ONLY");
			assertTrue(rs.next(), "Unable to show last 10 records in db after test");
			assertEquals(rowCnt, rs.getInt(1));
			assertEquals(String.format("test%-36s", rowCnt), rs.getString(2));
			System.out.println("10 last records in test table:");
			while (rs.next()) {
				System.out.printf("\t%02d: %s%n", rs.getInt(1), rs.getString(2));
			}
		}
    }
    
    @Test
    public void test01_HappyLoopPath() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("*********************").append(System.lineSeparator()) //
				.append("* Testing HappyPath *").append(System.lineSeparator()) //
				.append("*********************").append(System.lineSeparator()));

		try (Connection testConnection = testDataSource.getConnection();
		    	Statement testStatement = testConnection.createStatement()) {
			ResultSet rs = testStatement.executeQuery("SELECT count(1) from test");
			int recordCntAfter, recordCntBefore, rowCnt = BATCH_SIZE*BATCH_CNT;

			assertTrue(rs.next(), "Unable to count records in db before test");
			recordCntBefore = rs.getInt(1);

			doneEndpoint.expectedMessageCount(1);
	
	    	template.sendBody("direct:loop", -1);
	    	
	    	MockEndpoint.assertIsSatisfied(context, 120, TimeUnit.SECONDS);

			testConnection.setAutoCommit(true);

			rs.close();
			rs = testStatement.executeQuery("SELECT count(1) from test");
			
			assertTrue(rs.next(), "Unable to count records in db after test");
			recordCntAfter = rs.getInt(1);			
			assertEquals(rowCnt, recordCntAfter-recordCntBefore, "Unexpected number of rows in db");

    		rs.close();
			rs = testStatement.executeQuery("SELECT * from test OFFSET "+recordCntBefore+" ROWS FETCH FIRST 10 ROWS ONLY");
			assertTrue(rs.next(), "Unable to show first 10 records in db after test");
			assertEquals(recordCntBefore+1, rs.getInt(1));
			assertEquals(String.format("test%-36s", 1), rs.getString(2));
			System.out.println("10 first records in test table:");
			do {
				System.out.printf("\t%2d: %s%n", rs.getInt(1), rs.getString(2));
			} while (rs.next());

    		rs.close();
			rs = testStatement.executeQuery("SELECT * from test ORDER BY ROW_ID DESC FETCH FIRST 10 ROWS ONLY");
			assertTrue(rs.next(), "Unable to show last 10 records in db after test");
			assertEquals(rowCnt+recordCntBefore, rs.getInt(1));
			assertEquals(String.format("test%-36s", rowCnt), rs.getString(2));
			System.out.println("10 last records in test table:");
			while (rs.next()) {
				System.out.printf("\t%02d: %s%n", rs.getInt(1), rs.getString(2));
			}
		}
    }
    @Test
    public void test02_OnExceptionWithLoop() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("*********************************").append(System.lineSeparator()) //
				.append("* Testing OnException with Loop *").append(System.lineSeparator()) //
				.append("*********************************").append(System.lineSeparator()));

		try (Connection testConnection = testDataSource.getConnection();
				Statement testStatement = testConnection.createStatement()) {
			ResultSet rs = testStatement.executeQuery("SELECT count(1) from test");
			int recordCntAfter, recordCntBefore;
	    	Throwable t = null;

			assertTrue(rs.next(), "Unable to count records in db before test");
			recordCntBefore = rs.getInt(1);
    	
			exceptionEndpoint.expectedMessageCount(1);
	
	    	try {
	        	template.sendBody("direct:loop", 5);			
			} catch (CamelExecutionException e) {
				if (null != e.getCause() && (t = e.getCause()) instanceof RuntimeCamelException
						&& null != t.getCause() && (t = t.getCause()) instanceof RollbackExchangeException
						&& null != t.getCause() && (t = t.getCause()) instanceof RuntimeException) {
					System.err.printf("Caught a %s exception with message: %s%n", t.getClass(), t.getMessage());
				} else {
					t = null;
				}
			}
	    	
	    	MockEndpoint.assertIsSatisfied(context, 120, TimeUnit.SECONDS);
	    	
	    	assertNotEquals(t, "Expected exception not thrown");
	    	assertTrue(t.getMessage().startsWith("Failing with camelLoopIndex"), "Unexpected exception message: "+t.getMessage());
	    	
			rs.close();
			rs = testStatement.executeQuery("SELECT count(1) from test");

			assertTrue(rs.next(), "Unable to count records in db after test");
			recordCntAfter = rs.getInt(1);

			assertEquals(0, recordCntAfter - recordCntBefore, "Unexpected number of rows in db");
		}
   }
    
    @Test
    public void test03_HappySplitPath() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("**************************").append(System.lineSeparator()) //
				.append("* Testing HappySplitPath *").append(System.lineSeparator()) //
				.append("**************************").append(System.lineSeparator()));

		try (Connection testConnection = testDataSource.getConnection();
		    	Statement testStatement = testConnection.createStatement()) {
			ResultSet rs = testStatement.executeQuery("SELECT count(1) from test");
			int recordCntAfter, recordCntBefore, rowCnt = BATCH_SIZE*BATCH_CNT;

			assertTrue(rs.next(), "Unable to count records in db before test");
			recordCntBefore = rs.getInt(1);

			doneEndpoint.expectedMessageCount(1);
	
	    	template.sendBody("direct:split", -1);
	    	
	    	MockEndpoint.assertIsSatisfied(context, 120, TimeUnit.SECONDS);

			testConnection.setAutoCommit(true);

			rs.close();
			rs = testStatement.executeQuery("SELECT count(1) from test");
			
			assertTrue(rs.next(), "Unable to count records in db after test");
			recordCntAfter = rs.getInt(1);			
			assertEquals(rowCnt, recordCntAfter-recordCntBefore, "Unexpected number of rows in db");

    		rs.close();
			rs = testStatement.executeQuery("SELECT * from test OFFSET "+recordCntBefore+" ROWS FETCH FIRST 10 ROWS ONLY");
			assertTrue(rs.next(), "Unable to show first 10 records in db after test");
			assertEquals(recordCntBefore+1, rs.getInt(1));
			assertEquals(String.format("test%-36s", 1), rs.getString(2));
			System.out.println("10 first records in test table:");
			do {
				System.out.printf("\t%2d: %s%n", rs.getInt(1), rs.getString(2));
			} while (rs.next());

    		rs.close();
			rs = testStatement.executeQuery("SELECT * from test ORDER BY ROW_ID DESC FETCH FIRST 10 ROWS ONLY");
			assertTrue(rs.next(), "Unable to show last 10 records in db after test");
			assertEquals(rowCnt+recordCntBefore, rs.getInt(1));
			assertEquals(String.format("test%-36s", rowCnt), rs.getString(2));
			System.out.println("10 last records in test table:");
			while (rs.next()) {
				System.out.printf("\t%02d: %s%n", rs.getInt(1), rs.getString(2));
			}
		}
    }

    @Test
    public void test04_OnExceptionWithSplit() throws Exception {
		System.out.print(new StringBuilder(System.lineSeparator()) //
				.append("**********************************").append(System.lineSeparator()) //
				.append("* Testing OnException with Split *").append(System.lineSeparator()) //
				.append("**********************************").append(System.lineSeparator()));

		try (Connection testConnection = testDataSource.getConnection();
				Statement testStatement = testConnection.createStatement()) {
			ResultSet rs = testStatement.executeQuery("SELECT count(1) from test");
			int recordCntAfter, recordCntBefore;
	    	Throwable t = null;

			assertTrue(rs.next(), "Unable to count records in db before test");
			recordCntBefore = rs.getInt(1);
    	
			exceptionEndpoint.expectedMessageCount(1);
	
	    	try {
	        	template.sendBody("direct:split", 5);			
			} catch (CamelExecutionException e) {
				if (null != e.getCause() && (t = e.getCause()) instanceof RuntimeCamelException
						&& null != t.getCause() && (t = t.getCause()) instanceof RollbackExchangeException
						&& null != t.getCause() && (t = t.getCause()) instanceof CamelExchangeException
						&& null != t.getCause() && (t = t.getCause()) instanceof RuntimeException) {
					System.err.printf("Caught a %s exception with message: %s%n", t.getClass(), t.getMessage());
				} else {
					t = null;
				}
			}
	    	
	    	MockEndpoint.assertIsSatisfied(context, 120, TimeUnit.SECONDS);
	    	
	    	assertNotEquals(t, "Expected exception not thrown");
	    	assertTrue(t.getMessage().startsWith("Failing with splitIndex"), "Unexpected exception message: "+t.getMessage());
	    	
			rs.close();
			rs = testStatement.executeQuery("SELECT count(1) from test");

			assertTrue(rs.next(), "Unable to count records in db after test");
			recordCntAfter = rs.getInt(1);

			assertEquals(0, recordCntAfter - recordCntBefore, "Unexpected number of rows in db");
		}
   }
    
    
    @Override
    protected CamelContext createCamelContext() throws Exception {
    	CamelContext context = super.createCamelContext();
    	DataSourceTransactionManager transactionManager = new DataSourceTransactionManager(testDataSource);
    	context.getRegistry().bind("transactionManager", transactionManager);
    	
    	SpringTransactionPolicy txRequired = new SpringTransactionPolicy(transactionManager);
    	txRequired.setPropagationBehaviorName("PROPAGATION_REQUIRED");
    	context.getRegistry().bind("txRequired", txRequired);
    	
		ComponentsBuilderFactory.sql().dataSource(testDataSource)
			.register(context, "sql");
	   
        return context;
    }
    @Override
    protected RoutesBuilder createRouteBuilder() throws Exception {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {
				from("direct:loop")
					.transacted("txRequired")
					.setProperty("ThrowExeptionOnLoopIndex").body().convertBodyTo(Integer.class)
					.setProperty("MemoryUsageBeforeLoadingData", method(LoopTransactionTest.this, "getCurrentlyUsedMemory"))
//					.onException(Throwable.class)
//						.to("mock:exception")
//						.handled(false)
//					.end()
	            	.process(exchange -> {
						System.out.printf("Root exchange with hashCode '%s', id '%s' & transacted '%s'%n", exchange.hashCode(), exchange.getExchangeId(), exchange.isTransacted());
	            	})
					.doTry()
						.loop(BATCH_CNT).copy()
			            	.process(exchange -> {
								long memoryUsageAfterLoadingData, memoryUsageBeforeLoadingData = exchange.getProperty("MemoryUsageBeforeLoadingData", Long.class);
								Integer throwExeptionOnLoopIndex = exchange.getProperty("ThrowExeptionOnLoopIndex", Integer.class);
								Integer camelLoopIndex = exchange.getProperty("CamelLoopIndex", Integer.class);
			            		Message message = exchange.getMessage();
								List<String> elements = new ArrayList<>();
								int rowIdFrom = camelLoopIndex*BATCH_SIZE+1;
								int rowIdTo = (camelLoopIndex+1)*BATCH_SIZE;

								if (-1 != throwExeptionOnLoopIndex && throwExeptionOnLoopIndex == camelLoopIndex) {
									System.err.println("Failing with exception at camelLoopIndex: "+camelLoopIndex);
									throw new RuntimeException("Failing with camelLoopIndex: "+camelLoopIndex);
								}
								
								for (int rowId = rowIdFrom; rowId <= rowIdTo; rowId++) {
									elements.add(String.format("test%-36s", rowId));
								}
								
								memoryUsageAfterLoadingData = getCurrentlyUsedMemory();

								System.out.printf("Inserting %d elements in batch %d with %d MB memory used in loop exchange with hashCode '%s', id '%s' & transacted '%s'%n",
										null == elements?-1:elements.size(), camelLoopIndex+1, memoryUsageAfterLoadingData - memoryUsageBeforeLoadingData, 
												exchange.hashCode(), exchange.getExchangeId(), exchange.isTransacted());
								
								message.setBody(elements);
			            	})
			            	.to("sql:INSERT INTO test(TEST) VALUES(:#${body})?batch=true")
		            	.end() // .loop()
					.endDoTry()
					.doCatch(Throwable.class)
						.to("mock:exception")
						.process(exchange -> {
							Throwable t = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
							throw new RollbackExchangeException(exchange, t);	
						})
					.end() // .doCatch
	                .to("mock:done");
				
				from("direct:split")
					.transacted("txRequired")
					.setProperty("ThrowExeptionOnLoopIndex").body().convertBodyTo(Integer.class)
					.setProperty("MemoryUsageBeforeLoadingData", method(LoopTransactionTest.this, "getCurrentlyUsedMemory"))
					.process(exchange -> {
	            		Message message = exchange.getMessage();
						StringBuilder sb = new StringBuilder();
						
						for (int i = 0; i < BATCH_CNT; i++) {
							sb.append(i).append('\n');
						}
	
						message.setBody(sb.toString());
	
						System.out.printf("Root exchange with hashCode '%s', id '%s' & transacted '%s'%n", exchange.hashCode(), exchange.getExchangeId(), exchange.isTransacted());
					})
					.doTry()
						.split(body().tokenize("\n")).streaming().stopOnException()
							.process(exchange -> {
								Integer throwExeptionOnLoopIndex = exchange.getProperty("ThrowExeptionOnLoopIndex", Integer.class);
			            		Message message = exchange.getMessage();
								int splitIndex = message.getBody(Integer.class);
								int rowIdFrom = splitIndex*BATCH_SIZE+1;
								int rowIdTo = (splitIndex+1)*BATCH_SIZE;
								List<String> elements = new ArrayList<>();
			
	//							System.out.printf("Split exchange with hashCode '%s', id '%s' & transacted '%s'%n", exchange.hashCode(), exchange.getExchangeId(), exchange.isTransacted());
								
								if (-1 != throwExeptionOnLoopIndex && throwExeptionOnLoopIndex == splitIndex) {
									System.err.println("Failing with exception at splitIndex: "+splitIndex);
									throw new RuntimeException("Failing with splitIndex: "+splitIndex);
								}
								
								for (int rowId = rowIdFrom; rowId <= rowIdTo; rowId++) {
									elements.add(String.format("test%-36s", rowId));
								}
								
								message.setHeader("SplitIndex", splitIndex);
								message.setBody(elements);		
							})
			            	.to("sql:INSERT INTO test(TEST) VALUES(:#${body})?batch=true")
			            	.process(exchange -> {
								long memoryUsageAfterLoadingData, memoryUsageBeforeLoadingData = exchange.getProperty("MemoryUsageBeforeLoadingData", Long.class);
			            		Message message = exchange.getMessage();
			            		int splitIndex = message.getHeader("SplitIndex", Integer.class);
								@SuppressWarnings("unchecked")
								List<String> elements = message.getBody(List.class);
	
								memoryUsageAfterLoadingData = getCurrentlyUsedMemory();
	
								System.out.printf("Inserted %d elements in batch %d with %d MB memory used in loop exchange with hashCode '%s', id '%s' & transacted '%s'%n",
										null == elements?-1:elements.size(), splitIndex+1, memoryUsageAfterLoadingData - memoryUsageBeforeLoadingData, 
												exchange.hashCode(), exchange.getExchangeId(), exchange.isTransacted());
			            	})
						.end()
					.endDoTry()
					.doCatch(Throwable.class)
						.to("mock:exception")
						.process(exchange -> {
							Throwable t = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
							throw new RollbackExchangeException(exchange, t);	
						})
					.end() // .doCatch
	                .to("mock:done");

            }
        };
    }

    private static final long BYTE_TO_MB_CONVERSION_VALUE = 1024 * 1024;
	public Long getCurrentlyUsedMemory() {
		System.gc();
		return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / BYTE_TO_MB_CONVERSION_VALUE;
	}
}