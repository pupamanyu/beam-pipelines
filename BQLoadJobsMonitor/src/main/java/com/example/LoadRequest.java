package com.example;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.google.common.base.Objects;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"attributes", "payload"})
public class LoadRequest implements Serializable {

  @JsonProperty("attributes")
  public Attributes attributes;

  @JsonProperty("payload")
  public Payload payload;

  public Attributes getAttributes() {
    return attributes;
  }

  public void setAttributes(Attributes attributes) {
    this.attributes = attributes;
  }

  public Payload getPayload() {
    return payload;
  }

  public void setPayload(Payload payload) {
    this.payload = payload;
  }

  @Override
  public String toString() {
    return "LoadRequest{" + "attributes=" + attributes + ", payload=" + payload + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    LoadRequest that = (LoadRequest) o;
    return Objects.equal(getAttributes(), that.getAttributes())
        && Objects.equal(getPayload(), that.getPayload());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getAttributes(), getPayload());
  }
}
