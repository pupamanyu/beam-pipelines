package com.example;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"jobId", "jobCreatedTimestamp", "jobMonitoringStartTimeStamp", "jobMonitoringEndTimeStamp", "jobMonitoringTotalTimeMs", "jobCompleted", "jobAwaitingToRunLatencyMs", "jobRunLatencyMs", "jobTotalLatencyMs", "pushedBackForMonitoringRetries"})
public class LoaderEnvelopeAttributes implements Serializable {

  @JsonProperty("jobId")
  private String jobId;

  @JsonProperty("jobCreatedTimestamp")
  private Long jobCreatedTimestamp;

  @JsonProperty("jobMonitoringStartTimeStamp")
  private Long jobMonitoringStartTimeStamp;

  @JsonProperty("jobMonitoringEndTimeStamp")
  private Long jobMonitoringEndTimeStamp;

  @JsonProperty("jobMonitoringTotalTimeMs")
  private Long jobMonitoringTotalTimeMs;

  @JsonProperty("jobCompleted")
  private Boolean jobCompleted;

  @JsonProperty("jobAwaitingToRunLatencyMs")
  private Long jobAwaitingToRunLatencyMs;

  @JsonProperty("jobRunLatencyMs")
  private Long jobRunLatencyMs;

  @JsonProperty("jobTotalLatencyMs")
  private Long jobTotalLatencyMs;

  @JsonProperty("pushedBackForMonitoringRetries")
  private Integer pushedBackForMonitoringRetries;

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public Long getJobCreatedTimestamp() {
    return jobCreatedTimestamp;
  }

  public void setJobCreatedTimestamp(Long jobCreatedTimestamp) {
    this.jobCreatedTimestamp = jobCreatedTimestamp;
  }

  public Long getJobMonitoringStartTimeStamp() {
    return jobMonitoringStartTimeStamp;
  }

  public void setJobMonitoringStartTimeStamp(Long jobMonitoringStartTimeStamp) {
    this.jobMonitoringStartTimeStamp = jobMonitoringStartTimeStamp;
  }

  public Long getJobMonitoringEndTimeStamp() {
    return jobMonitoringEndTimeStamp;
  }

  public void setJobMonitoringEndTimeStamp(Long jobMonitoringEndTimeStamp) {
    this.jobMonitoringEndTimeStamp = jobMonitoringEndTimeStamp;
  }

  public Long getJobMonitoringTotalTimeMs() {
    return jobMonitoringTotalTimeMs;
  }

  public void setJobMonitoringTotalTimeMs(Long jobMonitoringTotalTimeMs) {
    this.jobMonitoringTotalTimeMs = jobMonitoringTotalTimeMs;
  }

  public Boolean getJobCompleted() {
    return jobCompleted;
  }

  public void setJobCompleted(Boolean jobCompleted) {
    this.jobCompleted = jobCompleted;
  }

  public Long getJobAwaitingToRunLatencyMs() {
    return jobAwaitingToRunLatencyMs;
  }

  public void setJobAwaitingToRunLatencyMs(Long jobAwaitingToRunLatencyMs) {
    this.jobAwaitingToRunLatencyMs = jobAwaitingToRunLatencyMs;
  }

  public Long getJobRunLatencyMs() {
    return jobRunLatencyMs;
  }

  public void setJobRunLatencyMs(Long jobRunLatencyMs) {
    this.jobRunLatencyMs = jobRunLatencyMs;
  }

  public Long getJobTotalLatencyMs() {
    return jobTotalLatencyMs;
  }

  public void setJobTotalLatencyMs(Long jobTotalLatencyMs) {
    this.jobTotalLatencyMs = jobTotalLatencyMs;
  }

  public Integer getPushedBackForMonitoringRetries() {
    return pushedBackForMonitoringRetries;
  }

  public void setPushedBackForMonitoringRetries(Integer pushedBackForMonitoringRetries) {
    this.pushedBackForMonitoringRetries = pushedBackForMonitoringRetries;
  }
}