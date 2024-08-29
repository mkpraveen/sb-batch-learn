package com.github.praveenmk.sbbatch.model;

import java.util.List;

import lombok.Data;

@Data
public class InvoicePayable {

    private InvoiceHeader invoiceHeader;
    private List<InvoiceDetail> invoiceDetails;

}
