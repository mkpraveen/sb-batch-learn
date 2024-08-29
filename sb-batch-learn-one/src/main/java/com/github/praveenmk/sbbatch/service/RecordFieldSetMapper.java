package com.github.praveenmk.sbbatch.service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import com.github.praveenmk.sbbatch.model.Transaction;

public class RecordFieldSetMapper implements FieldSetMapper<Transaction>{

    @Override
    public Transaction mapFieldSet(FieldSet fieldSet) throws BindException {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");

        Transaction transaction = new Transaction();
        // you can either use the indices or custom names
        // I personally prefer the custom names easy for debugging and
        // validating the pipelines
        transaction.setUsername(fieldSet.readString("username"));
        transaction.setUserId(fieldSet.readInt("userid"));
        transaction.setAmount(fieldSet.readDouble(4));
        transaction.setRecordId(fieldSet.readInt(0));

        // Converting the date
        String dateString = fieldSet.readString(3);
        transaction.setTransactionDate(LocalDate.parse(dateString, formatter).atStartOfDay());

        return transaction;
    }

}
