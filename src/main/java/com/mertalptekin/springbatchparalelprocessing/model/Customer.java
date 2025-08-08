package com.mertalptekin.springbatchparalelprocessing.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Customer {

    private Integer id;
    private String firstName;
    private String lastName;
    private Integer birthYear;

}
