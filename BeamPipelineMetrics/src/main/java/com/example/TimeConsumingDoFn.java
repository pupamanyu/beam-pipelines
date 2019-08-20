package com.example;

public class TimeConsumingDoFn extends WrapDoFn<Long, Void> {

  TimeConsumingDoFn(Options options) {
    super(options);
  }

  @Override void wrap(ProcessContext context) {
    Transaction checkoutTransaction = new Transaction();
    checkoutTransaction.execute();
  }
}
