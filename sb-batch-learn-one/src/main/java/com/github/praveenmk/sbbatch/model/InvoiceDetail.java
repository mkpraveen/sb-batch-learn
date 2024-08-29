package com.github.praveenmk.sbbatch.model;

import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class InvoiceDetail {
    List<Map<String, String>> attributeList;
}
