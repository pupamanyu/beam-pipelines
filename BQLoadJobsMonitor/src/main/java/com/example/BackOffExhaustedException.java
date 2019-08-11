package com.example;

class BackOffExhaustedException extends Exception {
  BackOffExhaustedException(String errorMessage) {
    super(errorMessage);
  }
}
