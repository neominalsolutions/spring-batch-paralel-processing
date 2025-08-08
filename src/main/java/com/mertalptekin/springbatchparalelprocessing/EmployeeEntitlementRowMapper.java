package com.mertalptekin.springbatchparalelprocessing;

import com.mertalptekin.springbatchparalelprocessing.model.EmployeeEntitlement;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class EmployeeEntitlementRowMapper implements RowMapper<EmployeeEntitlement> {

    @Override
    public EmployeeEntitlement mapRow(ResultSet rs, int rowNum) throws SQLException {
        EmployeeEntitlement ee = new EmployeeEntitlement();
        ee.setEmployeeId(rs.getInt("employee_id"));
        ee.setMonth(rs.getInt("_month"));
        ee.setYear(rs.getInt("_year"));
        ee.setTotalBonus(rs.getBigDecimal("total_bonus"));
        ee.setTotalDeduction(rs.getBigDecimal("total_deduction"));
        ee.setTotalPrepaidAmount(rs.getBigDecimal("total_prepaid_amount"));
        ee.setTotalIncome(rs.getBigDecimal("total_income"));
        ee.setCalculationDate(rs.getDate("calculation_date").toLocalDate()); // Date -> LocalDate dönüştürme
        return ee;
    }
}
