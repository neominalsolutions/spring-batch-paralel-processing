package com.mertalptekin.springbatchparalelprocessing.model;

// Aylık veya Yıllık Çalışan hakedişlerini tutan model sınıfı

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDate;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EmployeeEntitlement {

    private Integer employeeId;
    private Integer month;
    private Integer year;
    private BigDecimal totalBonus; // Prim toplamı
    private BigDecimal totalDeduction; // Kesinti toplamı
    private BigDecimal totalPrepaidAmount; // Avans toplamı
    private BigDecimal totalIncome; // Toplam kazanç (prim - kesinti + avans)
    private LocalDate calculationDate; // Hesaplama tarihi

}
