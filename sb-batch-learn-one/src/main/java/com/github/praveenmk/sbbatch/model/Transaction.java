package com.github.praveenmk.sbbatch.model;

import java.time.LocalDateTime;

import com.github.praveenmk.sbbatch.service.LocalDateTimeAdapter;

import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

@XmlRootElement(name = "transactionRecord")
public class Transaction {
    private int recordId;
    private String username;
    private int userId;
    private int age;
    private String postCode;
    private LocalDateTime transactionDate;
    private double amount;

    public String getUsername() {
        return username;
    }
    public void setUsername(String username) {
        this.username = username;
    }
    public int getUserId() {
        return userId;
    }
    public void setUserId(int userId) {
        this.userId = userId;
    }
    public int getAge() {
        return age;
    }
    public void setAge(int age) {
        this.age = age;
    }
    public String getPostCode() {
        return postCode;
    }
    public void setPostCode(String postCode) {
        this.postCode = postCode;
    }
    
    @XmlJavaTypeAdapter(LocalDateTimeAdapter.class)
    public LocalDateTime getTransactionDate() {
        return transactionDate;
    }
    public void setTransactionDate(LocalDateTime transactionDate) {
        this.transactionDate = transactionDate;
    }
    public double getAmount() {
        return amount;
    }
    public void setAmount(double amount) {
        this.amount = amount;
    }

    public int getRecordId() {
        return recordId;
    }
    public void setRecordId(int recordId) {
        this.recordId = recordId;
    }
    @Override
    public String toString() {
        long threadId = Thread.currentThread().getId();
        return "Thread:" + threadId + " , Transaction [recordId=" + recordId + ", username=" + username + ", userId=" + userId + ", age=" + age
                + ", postCode=" + postCode + ", transactionDate=" + transactionDate + ", amount=" + amount + "]";
    }
   
}
