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
public class Payload implements Serializable {

  @JsonProperty("bundleId")
  public String bundleId;

  @JsonProperty("bundlePrefixPath")
  public String bundlePrefixPath;

  @JsonProperty("bundleSize")
  public String bundleSize;

  @JsonProperty("bundleCount")
  public String bundleCount;

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

  public String getBundleSize() {
    return bundleSize;
  }

  public void setBundleSize(String bundleSize) {
    this.bundleSize = bundleSize;
  }

  public String getBundleCount() {
    return bundleCount;
  }

  public void setBundleCount(String bundleCount) {
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
    Payload payload = (Payload) o;
    return Objects.equal(getBundleId(), payload.getBundleId())
        && Objects.equal(getBundlePrefixPath(), payload.getBundlePrefixPath())
        && Objects.equal(getBundleSize(), payload.getBundleSize())
        && Objects.equal(getBundleCount(), payload.getBundleCount())
        && Objects.equal(getBundleDataset(), payload.getBundleDataset())
        && Objects.equal(getBundleTable(), payload.getBundleTable())
        && Objects.equal(getBundleSchemaVersion(), payload.getBundleSchemaVersion())
        && Objects.equal(getBundleSchemaHash(), payload.getBundleSchemaHash());
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
    return "Payload{"
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
