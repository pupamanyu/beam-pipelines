package com.example;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Objects;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
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

  @JsonProperty("bundlePrefixPath")
  public String bundlePrefixPath;

  @JsonProperty("bundleSize")
  public String bundleSize;

  @JsonProperty("bundleCount")
  public String bundleCount;

  @JsonProperty("bundleProject")
  public String bundleProject;

  @JsonProperty("bundleDataset")
  public String bundleDataset;

  @JsonProperty("bundleTable")
  public String bundleTable;

  @JsonProperty("bundleSchemaVersion")
  public String bundleSchemaVersion;

  @JsonProperty("bundleSchemaHash")
  public String bundleSchemaHash;

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

  public String getBundleProject() {
    return bundleProject;
  }

  public void setBundleProject(String bundleProject) {
    this.bundleProject = bundleProject;
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
  public String toString() {
    return "Payload{"
        + "bundlePrefixPath='"
        + bundlePrefixPath
        + '\''
        + ", bundleSize='"
        + bundleSize
        + '\''
        + ", bundleCount='"
        + bundleCount
        + '\''
        + ", bundleProject='"
        + bundleProject
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Payload payload = (Payload) o;
    return Objects.equal(getBundlePrefixPath(), payload.getBundlePrefixPath())
        && Objects.equal(getBundleSize(), payload.getBundleSize())
        && Objects.equal(getBundleCount(), payload.getBundleCount())
        && Objects.equal(getBundleProject(), payload.getBundleProject())
        && Objects.equal(getBundleDataset(), payload.getBundleDataset())
        && Objects.equal(getBundleTable(), payload.getBundleTable())
        && Objects.equal(getBundleSchemaVersion(), payload.getBundleSchemaVersion())
        && Objects.equal(getBundleSchemaHash(), payload.getBundleSchemaHash());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        getBundlePrefixPath(),
        getBundleSize(),
        getBundleCount(),
        getBundleProject(),
        getBundleDataset(),
        getBundleTable(),
        getBundleSchemaVersion(),
        getBundleSchemaHash());
  }
}
