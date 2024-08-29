package com.github.praveenmk.sbbatch.service;

import org.springframework.batch.item.ItemProcessor;
import com.github.praveenmk.sbbatch.model.Transaction;

public class CustomItemProcessor implements ItemProcessor<Transaction, Transaction> {

    public Transaction process(Transaction item) {
        System.out.println("Processing..." + item);
        return item;
    }
}
