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
import com.google.auto.value.AutoValue;

@AutoValue @JsonInclude(JsonInclude.Include.NON_EMPTY) @JsonDeserialize(builder = AutoValue_State.Builder.class) @JsonIgnoreProperties(ignoreUnknown = true) public abstract class State {

  static Builder newBuilder() {
    return new AutoValue_State.Builder();
  }

  @JsonProperty("Key") public abstract String Key();

  @JsonProperty("Field1") public abstract String Field1();

  @JsonProperty("Field2") public abstract String Field2();

  @AutoValue.Builder public interface Builder {

    @JsonProperty("Key") Builder Key(String Key);

    @JsonProperty("Field1") Builder Field1(String Field1);

    @JsonProperty("Field2") Builder Field2(String Field2);

    State build();
  }
}
