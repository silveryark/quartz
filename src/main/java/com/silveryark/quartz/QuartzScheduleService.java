package com.silveryark.quartz;

import com.silveryark.utils.Dates;
import com.silveryark.utils.Randoms;
import com.silveryark.utils.Services;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

@Service
public class QuartzScheduleService implements ScheduleService {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuartzScheduleService.class);
    private static final String SPLITER = "/";
    private static final String SERVICE_KEY = "_service_key";
    private static final String JOB_KEY = "_job_key";
    private static final String JOB_HEADER = "x-job-key";
    private final Scheduler scheduler;
    private final Dates dates;
    private final Randoms randoms;
    private final Services services;
    @Value("${server.name}")
    private String serverName;
    @Value("${server.group.id}")
    private String groupId;
    @Value("${server.group.name}")
    private String groupName;
    @Value("${server.group.version}")
    private String groupVersion;

    @Autowired
    QuartzScheduleService(Scheduler scheduler, Dates dates, Randoms randoms, Services services)
            throws SchedulerException {
        this.scheduler = scheduler;
        this.dates = dates;
        this.randoms = randoms;
        this.services = services;

        scheduler.start();
    }

    @Override
    public String scheduleJob(Map<String, Object> data, long delayInSeconds, String service) throws SchedulerException {
        String uniqueKey = randoms.randomAlphanumeric(16);
        LocalDateTime now = dates.now();
        LocalDateTime runningTime;
        if (delayInSeconds > 0) {
            runningTime = now.plusSeconds(delayInSeconds);
        } else {
            runningTime = now;
        }
        JobKey jobKey = new JobKey(String.format("job.%s", uniqueKey), String.format("%s.%s.%s.%s", groupName, groupId,
                groupVersion, serverName));
        LOGGER.trace("Schedule one time job {} at {}, for key {}", CallerJob.class, runningTime, uniqueKey);
        //通过uniqueKey和分组信息生成job
        JobDataMap dataMap = new JobDataMap(data);
        dataMap.put(SERVICE_KEY, services.getService(service));
        dataMap.put(JOB_KEY, jobKey.getGroup() + SPLITER + jobKey.getName());
        JobDetail job = newJob(CallerJob.class)
                .withIdentity(jobKey)
                .usingJobData(dataMap)
                .build();
        Trigger trigger = newTrigger()
                .withIdentity(String.format("trigger.%s", uniqueKey), String.format("%s.%s.%s.%s", groupName, groupId,
                        groupVersion, serverName))
                .startAt(Date.from(runningTime.atZone(ZoneId.systemDefault()).toInstant()))
                .build();
        // Tell quartz to schedule the job using our trigger
        scheduler.scheduleJob(job, trigger);
        return jobKey.getGroup() + SPLITER + jobKey.getName();
    }

    @Override
    public void cancelJob(String jobKey) throws SchedulerException {
        String[] split = jobKey.split(SPLITER);
        JobKey key = JobKey.jobKey(split[1], split[0]);
        scheduler.deleteJob(key);
    }

    public static class CallerJob implements Job {

        private WebClient.Builder clientBuilder;

        public CallerJob() {
            clientBuilder = WebClient.builder()
                    .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_UTF8.toString())
                    .defaultHeader(HttpHeaders.USER_AGENT, "quartz");
        }

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            JobDataMap dataMap = context.getMergedJobDataMap();
            Map<String, Object> wrappedMap = dataMap.getWrappedMap();
            String jobKey = String.valueOf(wrappedMap.remove(JOB_KEY));
            String serviceEndpoint = String.valueOf(wrappedMap.remove(SERVICE_KEY));
            WebClient client = clientBuilder.baseUrl(serviceEndpoint).build();
            client
                    .post()
                    .uri("/quartz")
                    .header(JOB_HEADER, jobKey)
                    .body(BodyInserters.fromObject(wrappedMap))
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();
        }
    }
}
