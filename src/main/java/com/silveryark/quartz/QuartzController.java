package com.silveryark.quartz;

import com.silveryark.rpc.GenericRequest;
import com.silveryark.rpc.GenericResponse;
import com.silveryark.rpc.RPCResponse;
import com.silveryark.rpc.quartz.QuartzRequest;
import com.silveryark.rpc.quartz.QuartzResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController("/quartz")
public class QuartzController {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuartzController.class);

    private final ScheduleService scheduleService;

    @Autowired
    public QuartzController(ScheduleService scheduleService) {
        this.scheduleService = scheduleService;
    }

    @PostMapping("/job")
    public Mono<QuartzResponse> createJob(@RequestBody QuartzRequest request) {
        QuartzRequest.QuartzPayload payload = request.getPayload();
        try {
            String jobKey = scheduleService.scheduleJob(payload.getData(), payload.getDelayInSeconds(),
                    payload.getService());
            return Mono.just(new QuartzResponse(request.getRequestId(), RPCResponse.STATUS.OK, jobKey, null));
        } catch (Exception e) {
            LOGGER.error("error when create quartz job", e);
            return Mono.just(new QuartzResponse(request.getRequestId(), RPCResponse.STATUS.SERVER_ERROR, null, e));
        }
    }

    @DeleteMapping("/job/{id}")
    public Mono<GenericResponse> deleteJob(@PathVariable("id") String id, @RequestBody GenericRequest request) {
        try {
            scheduleService.cancelJob(id);
            return Mono.just(new GenericResponse(request.getRequestId(), RPCResponse.STATUS.OK, true));
        } catch (Exception e) {
            return Mono.just(new GenericResponse(request.getRequestId(), RPCResponse.STATUS.SERVER_ERROR, e));
        }
    }
}
