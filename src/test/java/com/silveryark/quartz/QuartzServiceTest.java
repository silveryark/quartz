package com.silveryark.quartz;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.silveryark.utils.Dates;
import com.silveryark.utils.Randoms;
import com.silveryark.utils.Services;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.mock.action.ExpectationCallback;
import org.mockserver.model.HttpClassCallback;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockserver.model.HttpRequest.request;

@RunWith(SpringRunner.class)
public class QuartzServiceTest {

    public static final String SERVICE_NAME = "anyService";
    private static ObjectMapper jsonMapper;
    private static CountDownLatch latch;
    private static ClientAndServer mockServer;
    private static int port;
    private static Map<String, Object> body;
    private Scheduler scheduler;
    @Mock
    private Dates dates;
    @Mock
    private Randoms randoms;
    @Mock
    private Services services;
    private QuartzScheduleService service;

    @AfterClass
    public static void destroyGlobal() {
        mockServer.stop();
    }

    @BeforeClass
    public static void setupGlobal() {
        port = new Random().nextInt(1024) + 1024;
        mockServer = ClientAndServer.startClientAndServer(port);
        body = new HashMap<>();
        body.put(RandomStringUtils.randomAlphabetic(16), RandomStringUtils.randomAlphanumeric(16));
        jsonMapper = new ObjectMapper();
    }

    @Before
    public void setup() throws SchedulerException, JsonProcessingException {
        latch = new CountDownLatch(1);
        scheduler = new StdSchedulerFactory("quartz.test.properties").getScheduler();
        scheduler.start();
        Mockito.when(dates.now()).thenCallRealMethod();
        Mockito.when(randoms.randomAlphanumeric(Mockito.anyInt())).thenCallRealMethod();
        Mockito.when(services.getService(Mockito.anyString())).thenReturn("http://localhost:" + port);
        service = new QuartzScheduleService(scheduler, dates, randoms, services);
        new MockServerClient("localhost", port)
                .when(request()
                        .withMethod("POST")
                        .withBody(jsonMapper.writeValueAsString(body)))
                .callback(HttpClassCallback.callback().withCallbackClass(Callback.class.getName()));
    }

    @After
    public void after() {
        Mockito.reset(dates, randoms, services);
    }

    @Test
    public void testOnetimeJob() throws SchedulerException, InterruptedException {
        long delayInSeconds = 1;
        service.scheduleJob(body, delayInSeconds, SERVICE_NAME);
        boolean await = latch.await(delayInSeconds + 1, TimeUnit.SECONDS);
        Assert.assertTrue("should return normally", await);
    }

    @Test
    public void testInstantJob() throws SchedulerException, InterruptedException {
        service.scheduleJob(body, -1, SERVICE_NAME);
        boolean await = latch.await(1, TimeUnit.SECONDS);
        Assert.assertTrue("should return normally", await);
    }

    @Test
    public void testCancelJob() throws SchedulerException, InterruptedException {
        long longTimeDelay = 1;
        String jobKey = service.scheduleJob(body, longTimeDelay, SERVICE_NAME);
        service.cancelJob(jobKey);
        boolean await = latch.await(longTimeDelay + 1, TimeUnit.SECONDS);
        Assert.assertFalse("latch should not count down", await);
    }

    @Test
    public void testCancelExpiredJob() throws SchedulerException, InterruptedException {
        long longTimeDelay = 1;
        String jobKey = service.scheduleJob(body, longTimeDelay, SERVICE_NAME);
        boolean await = latch.await(longTimeDelay + 1, TimeUnit.SECONDS);
        service.cancelJob(jobKey);
        Assert.assertTrue("latch should count down", await);
    }

    public static class Callback implements ExpectationCallback {

        @Override
        public HttpResponse handle(HttpRequest httpRequest) {
            latch.countDown();
            return HttpResponse.response().withStatusCode(200);
        }
    }

}
