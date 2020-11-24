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
public class EhrOdsJobDirectory {
    private String JobCoding;
    private String JobName;
    private String Process;
    private String DirectClassification;
    private String JobType;

    public EhrOdsJobDirectory(String jobCoding, String jobName, String process, String directClassification, String jobType) {
        JobCoding = jobCoding;
        JobName = jobName;
        Process = process;
        DirectClassification = directClassification;
        JobType = jobType;
    }
}
