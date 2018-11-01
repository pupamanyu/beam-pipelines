package com.example;

import org.junit.jupiter.api.Test;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.jupiter.api.Assertions;

class Testing {

  @Test
  void testJSONSanitization() throws IOException {
    String wrongProp = "weird prop: 2.23";
    String correctProp = "weird_prop_2_23";
    JsonNode json = new ObjectMapper().readTree(String.format("{\"%s\":5}", wrongProp));
    json = TransformJsonDoFn.sanitizePropertyNames(json);
    Assertions.assertTrue(json.has(correctProp));
    Assertions.assertFalse(json.has(wrongProp));
  }

  @Test
  void testStringify() throws IOException {
    JsonNode json = new ObjectMapper().readTree("{\"object\":{\"dummy\":345}}");
    String field = "object";
    json = TransformJsonDoFn.stringifyCustomData(json, field);
    Assertions.assertTrue(json.get(field).isTextual());
  }

}
