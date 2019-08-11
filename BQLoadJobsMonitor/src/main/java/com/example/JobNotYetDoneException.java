package com.example;

class JobNotYetDoneException extends Exception {
  JobNotYetDoneException(String errorMessage) {
    super(errorMessage);
  }
}
