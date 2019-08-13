package com.example;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Objects;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "bundleId",
    "bundlePrefixPath",
    "bundleSize",
    "bundleCount",
    "bundleProject",
    "bundleDataset",
    "bundleTable",
    "bundleSchemaVersion",
    "bundleSchemaHash"
})
public class LoadRequestPayload implements Serializable {

  @JsonProperty("bundleId")
  public String bundleId;

  @JsonProperty("bundlePrefixPath")
  public String bundlePrefixPath;

  @JsonProperty("bundleSize")
  public Long bundleSize;

  @JsonProperty("bundleCount")
  public Integer bundleCount;

  @JsonProperty("bundleDataset")
  public String bundleDataset;

  @JsonProperty("bundleTable")
  public String bundleTable;

  @JsonProperty("bundleSchemaVersion")
  public String bundleSchemaVersion;

  @JsonProperty("bundleSchemaHash")
  public String bundleSchemaHash;

  public String getBundleId() {
    return bundleId;
  }

  public void setBundleId(String bundleId) {
    this.bundleId = bundleId;
  }

  public String getBundlePrefixPath() {
    return bundlePrefixPath;
  }

  public void setBundlePrefixPath(String bundlePrefixPath) {
    this.bundlePrefixPath = bundlePrefixPath;
  }

  public Long getBundleSize() {
    return bundleSize;
  }

  public void setBundleSize(Long bundleSize) {
    this.bundleSize = bundleSize;
  }

  public Integer getBundleCount() {
    return bundleCount;
  }

  public void setBundleCount(Integer bundleCount) {
    this.bundleCount = bundleCount;
  }

  public String getBundleDataset() {
    return bundleDataset;
  }

  public void setBundleDataset(String bundleDataset) {
    this.bundleDataset = bundleDataset;
  }

  public String getBundleTable() {
    return bundleTable;
  }

  public void setBundleTable(String bundleTable) {
    this.bundleTable = bundleTable;
  }

  public String getBundleSchemaVersion() {
    return bundleSchemaVersion;
  }

  public void setBundleSchemaVersion(String bundleSchemaVersion) {
    this.bundleSchemaVersion = bundleSchemaVersion;
  }

  public String getBundleSchemaHash() {
    return bundleSchemaHash;
  }

  public void setBundleSchemaHash(String bundleSchemaHash) {
    this.bundleSchemaHash = bundleSchemaHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LoadRequestPayload loadRequestPayload = (LoadRequestPayload) o;
    return Objects.equal(getBundleId(), loadRequestPayload.getBundleId())
        && Objects.equal(getBundlePrefixPath(), loadRequestPayload.getBundlePrefixPath())
        && Objects.equal(getBundleSize(), loadRequestPayload.getBundleSize())
        && Objects.equal(getBundleCount(), loadRequestPayload.getBundleCount())
        && Objects.equal(getBundleDataset(), loadRequestPayload.getBundleDataset())
        && Objects.equal(getBundleTable(), loadRequestPayload.getBundleTable())
        && Objects.equal(getBundleSchemaVersion(), loadRequestPayload.getBundleSchemaVersion())
        && Objects.equal(getBundleSchemaHash(), loadRequestPayload.getBundleSchemaHash());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getBundleId(),
        getBundlePrefixPath(),
        getBundleSize(),
        getBundleCount(),
        getBundleDataset(),
        getBundleTable(),
        getBundleSchemaVersion(),
        getBundleSchemaHash());
  }

  @Override
  public String toString() {
    return "LoadRequestPayload{"
        + "bundleId='"
        + bundleId
        + '\''
        + ", bundlePrefixPath='"
        + bundlePrefixPath
        + '\''
        + ", bundleSize='"
        + bundleSize
        + '\''
        + ", bundleCount='"
        + bundleCount
        + '\''
        + ", bundleDataset='"
        + bundleDataset
        + '\''
        + ", bundleTable='"
        + bundleTable
        + '\''
        + ", bundleSchemaVersion='"
        + bundleSchemaVersion
        + '\''
        + ", bundleSchemaHash='"
        + bundleSchemaHash
        + '\''
        + '}';
  }
}

