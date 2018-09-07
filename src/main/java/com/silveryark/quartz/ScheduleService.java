package com.silveryark.quartz;

import java.util.Map;

public interface ScheduleService {

    /**
     * 设定一个集群化的定时任务
     *
     * @param data           执行job的时候所需要的参数
     * @param delayInSeconds 延迟多久执行，传0就马上执行
     * @param service        执行具体job的服务，path部份约定使用/quartz
     * @return JobKey，用于cancel一个Job
     */
    String scheduleJob(Map<String, Object> data, long delayInSeconds, String service) throws Exception;

    /**
     * 取消一个定时任务
     *
     * @param jobKey 定时任务标识
     * @return true, 取消成功， false，取消失败（有可能已经执行，有可能别的原因失败）
     */
    void cancelJob(String jobKey) throws Exception;
}
