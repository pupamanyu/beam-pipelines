/*
 * Copyright (c) 2020 Google Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.example;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@AutoValue
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonDeserialize(builder = AutoValue_Metric.Builder.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class Metric {
  static Builder newBuilder() {
    return new AutoValue_Metric.Builder();
  }

  @JsonProperty("TestName")
  public abstract String TestName();

  @JsonProperty("TestURL")
  public abstract String TestURL();

  @JsonProperty("TimeStamp")
  @JsonSerialize(using = CustomTimeStampSerializer.class)
  public abstract LocalDateTime TimeStamp();

  @JsonProperty("NodeName")
  public abstract String NodeName();

  @JsonProperty("DNSTime")
  public abstract BigDecimal DNSTime();

  @JsonProperty("Connect")
  public abstract BigDecimal Connect();

  @JsonProperty("SSL")
  public abstract BigDecimal SSL();

  @JsonProperty("SendTime")
  public abstract BigDecimal SendTime();

  @JsonProperty("WaitTime")
  public abstract BigDecimal WaitTime();

  @JsonProperty("Total")
  public abstract BigDecimal Total();

  @JsonProperty("PacketLoss")
  public abstract BigDecimal PacketLoss();

  @JsonProperty("RTTAvg")
  public abstract BigDecimal RTTAvg();

  @JsonProperty("nodeDetails")
  public abstract String nodeDetails();

  @JsonProperty("TraceRouteAddress")
  public abstract String TraceRouteAddress();

  @JsonProperty("pingRTT")
  public abstract BigDecimal pingRTT();

  @JsonProperty("ErrorCode")
  public abstract String ErrorCode();

  @AutoValue.Builder
  public interface Builder {
    @JsonProperty("TestName")
    Builder TestName(String TestName);

    @JsonProperty("TestURL")
    Builder TestURL(String TestURL);

    @JsonProperty("TimeStamp")
    @JsonDeserialize(as = LocalDateTime.class, using = CustomTimeStampDeserializer.class)
    Builder TimeStamp(LocalDateTime TimeStamp);

    @JsonProperty("NodeName")
    Builder NodeName(String NodeName);

    @JsonProperty("DNSTime")
    Builder DNSTime(BigDecimal DNSTime);

    @JsonProperty("Connect")
    Builder Connect(BigDecimal Connect);

    @JsonProperty("SSL")
    Builder SSL(BigDecimal SSL);

    @JsonProperty("SendTime")
    Builder SendTime(BigDecimal SendTime);

    @JsonProperty("WaitTime")
    Builder WaitTime(BigDecimal WaitTime);

    @JsonProperty("Total")
    Builder Total(BigDecimal Total);

    @JsonProperty("PacketLoss")
    Builder PacketLoss(BigDecimal PacketLoss);

    @JsonProperty("RTTAvg")
    Builder RTTAvg(BigDecimal RTTAvg);

    @JsonProperty("nodeDetails")
    Builder nodeDetails(String nodeDetails);

    @JsonProperty("TraceRouteAddress")
    Builder TraceRouteAddress(String TraceRouteAddress);

    @JsonProperty("pingRTT")
    Builder pingRTT(BigDecimal pingRTT);

    @JsonProperty("ErrorCode")
    Builder ErrorCode(String ErrorCode);

    Metric build();
  }
}
