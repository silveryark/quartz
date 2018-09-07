package com.silveryark.quartz;

import com.silveryark.rpc.GenericRequest;
import com.silveryark.rpc.GenericResponse;
import com.silveryark.rpc.RPCResponse;
import com.silveryark.rpc.quartz.QuartzRequest;
import com.silveryark.rpc.quartz.QuartzResponse;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.context.junit4.SpringRunner;
import reactor.core.publisher.Mono;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RunWith(SpringRunner.class)
public class QuartzControllerTest {
    @Mock
    ScheduleService scheduleService;
    QuartzController controller;
    private String jobKey;

    @Before
    public void before() throws Exception {
        jobKey = RandomStringUtils.randomAlphabetic(16);
        Mockito.when(scheduleService.scheduleJob(Mockito.anyMap(), Mockito.anyLong(), Mockito.anyString())).thenReturn(jobKey);
        Mockito.when(scheduleService.scheduleJob(Mockito.anyMap(), Mockito.anyLong(), Mockito.eq("exceptionService"))).thenThrow(new RuntimeException("Ops."));
        Mockito.doAnswer((key) -> null).when(scheduleService).cancelJob(jobKey);
        Mockito.doThrow(new RuntimeException("Ops.")).when(scheduleService).cancelJob(Mockito.eq("exception"));
        controller = new QuartzController(scheduleService);
    }

    @After
    public void after() {
        Mockito.reset(scheduleService);
    }

    @Test
    public void testCreateJob() {
        Map<String, Object> data = new HashMap<>();
        long delayInSeconds = new Random().nextLong();
        String service = RandomStringUtils.randomAlphanumeric(16);
        Mono<QuartzResponse> job = controller.createJob(new QuartzRequest(new QuartzRequest.QuartzPayload(data, delayInSeconds, service)));
        job.map((QuartzResponse response) -> {
            Assert.assertEquals(jobKey, response.getPayload());
            return true;
        }).block();
    }

    @Test
    public void testFailJob() {
        Map<String, Object> data = new HashMap<>();
        long delayInSeconds = new Random().nextLong();
        String service = "exceptionService";
        Mono<QuartzResponse> job = controller.createJob(new QuartzRequest(new QuartzRequest.QuartzPayload(data, delayInSeconds, service)));
        job.map((QuartzResponse response) -> {
            Assert.assertSame(RPCResponse.STATUS.SERVER_ERROR, response.getStatus());
            return true;
        }).block();
    }

    @Test
    public void testDeleteJob() {
        Mono<GenericResponse> deleteJob = controller.deleteJob(jobKey, new GenericRequest(null));
        deleteJob.map((GenericResponse response) -> {
            Assert.assertSame(RPCResponse.STATUS.OK, response.getStatus());
            return true;
        }).block();
    }

    @Test
    public void testFailDeleteJob() {
        Mono<GenericResponse> deleteJob = controller.deleteJob("exception", new GenericRequest(null));
        deleteJob.map((GenericResponse response) -> {
            Assert.assertSame(RPCResponse.STATUS.SERVER_ERROR, response.getStatus());
            return true;
        }).block();
    }
}
