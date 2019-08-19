package com.example;

import java.util.concurrent.ThreadLocalRandom;

class Transaction {

  void execute() {
    try {
      Thread.sleep(ThreadLocalRandom.current().nextInt(10, 1000));
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
