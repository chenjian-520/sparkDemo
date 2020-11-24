package com.foxconn.dpm.sprint1_2.ods_dwd.beans;

import lombok.Data;

/**
 * Description:  com.dl.spark.ehr.dwd.dto
 * Copyright: © 2020 Foxconn. All rights reserved.
 * Company: Foxconn
 *
 * @author FL
 * @version 1.0
 * @timestamp 2020/1/3
 */
@Data
public class EhrOdsMbu {
    private String Department;
    private String Product;
    private String FunctionDepartment;
    private String LevelFunctionDepartment;
    private String Site;
    private String empId;
    private String factory_code;

    public EhrOdsMbu(String department, String product, String functionDepartment, String levelFunctionDepartment, String site,String factoryCode) {
        Department = department;
        Product = product;
        FunctionDepartment = functionDepartment;
        LevelFunctionDepartment = levelFunctionDepartment;
        Site = site;
        factory_code = factoryCode;
    }
//    public EhrOdsMbu(String department, String product, String functionDepartment, String levelFunctionDepartment, String site) {
//        Department = department;
//        Product = product;
//        FunctionDepartment = functionDepartment;
//        LevelFunctionDepartment = levelFunctionDepartment;
//        Site = site;
//    }
}
