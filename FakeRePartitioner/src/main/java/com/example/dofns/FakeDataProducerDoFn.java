package com.example.dofns;

import com.github.javafaker.Faker;
import com.github.javafaker.service.FakeValuesService;
import com.github.javafaker.service.RandomService;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.Locale;

public class FakeDataProducerDoFn extends DoFn<Long, TableRow> {
  private transient FakeValuesService fakeValuesService;
  private transient Faker faker;
  private transient TableRow tableRow;

  @Setup
  public void doSetup() {
    this.fakeValuesService = new FakeValuesService(new Locale("en-US"), new RandomService());
    this.faker = new Faker();
    this.tableRow = new TableRow();
  }

  @ProcessElement
  public void processElement(@Element Long row, OutputReceiver<TableRow> outputReceiver) {
    String email = this.fakeValuesService.bothify("???????###@gmail.com");
    String uid = this.fakeValuesService.regexify("[a-z][a-z1-9]{9}");
    String streetName = this.faker.address().streetName();
    String buildingNumber = this.faker.address().buildingNumber();
    String city = this.faker.address().city();
    String country = this.faker.address().country();
    String firstName = this.faker.address().firstName();
    String lastName = this.faker.address().lastName();
    this.tableRow.set("email", email);
    this.tableRow.set("uid", uid);
    this.tableRow.set("streetName", streetName);
    this.tableRow.set("buildingNumber", buildingNumber);
    this.tableRow.set("city", city);
    this.tableRow.set("country", country);
    this.tableRow.set("firstName", firstName);
    this.tableRow.set("lastName", lastName);
    outputReceiver.output(tableRow);
  }
}
