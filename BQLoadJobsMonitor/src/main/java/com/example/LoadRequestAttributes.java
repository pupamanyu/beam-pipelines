package com.example;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"bundleCreatedTimestamp", "loadJobSubmissionAttempts", "retryAttemptsAfterJobFailures", "previousFailedJobIds"})
public class LoadRequestAttributes implements Serializable {
  @JsonProperty("bundleCreatedTimestamp")
  private Long bundleCreatedTimestamp;

  @JsonProperty("loadJobSubmissionAttempts")
  private Integer loadJobSubmissionAttempts;

  @JsonProperty("retryAttemptsAfterJobFailures")
  private Integer retryAttemptsAfterJobFailures;

  @JsonProperty("previousFailedJobIds")
  private List<String> previousFailedJobIds;

  public Long getBundleCreatedTimestamp() {
    return bundleCreatedTimestamp;
  }

  public void setBundleCreatedTimestamp(Long bundleCreatedTimestamp) {
    this.bundleCreatedTimestamp = bundleCreatedTimestamp;
  }

  public Integer getLoadJobSubmissionAttempts() {
    return loadJobSubmissionAttempts;
  }

  public void setLoadJobSubmissionAttempts(Integer loadJobSubmissionAttempts) {
    this.loadJobSubmissionAttempts = loadJobSubmissionAttempts;
  }

  public Integer getRetryAttemptsAfterJobFailures() {
    return retryAttemptsAfterJobFailures;
  }

  public void setRetryAttemptsAfterJobFailures(Integer retryAttemptsAfterJobFailures) {
    this.retryAttemptsAfterJobFailures = retryAttemptsAfterJobFailures;
  }

  public List<String> getPreviousFailedJobIds() {
    return previousFailedJobIds;
  }

  public void setPreviousFailedJobIds(List<String> previousFailedJobIds) {
    this.previousFailedJobIds = previousFailedJobIds;
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    LoadRequestAttributes that = (LoadRequestAttributes) o;
    return Objects.equal(getBundleCreatedTimestamp(), that.getBundleCreatedTimestamp()) && Objects
        .equal(getLoadJobSubmissionAttempts(), that.getLoadJobSubmissionAttempts()) && Objects
        .equal(getRetryAttemptsAfterJobFailures(), that.getRetryAttemptsAfterJobFailures());
  }

  @Override public int hashCode() {
    return Objects.hashCode(getBundleCreatedTimestamp(), getLoadJobSubmissionAttempts(),
        getRetryAttemptsAfterJobFailures());
  }

  @Override public String toString() {
    return "LoadRequestAttributes{" + "bundleCreatedTimestamp='" + bundleCreatedTimestamp + '\''
        + ", loadJobSubmissionAttempts='" + loadJobSubmissionAttempts + '\''
        + ", retryAttemptsAfterJobFailures='" + retryAttemptsAfterJobFailures + '\'' + '}';
  }
}
