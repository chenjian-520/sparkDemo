## 需执行的 spark 任务
### dwd -> dws
- 启动类：
com.foxconn.dpm.sprint5.dwd_dws.controller.DwsDowntimeSpringFiveController
- 业务实现类：
com.foxconn.dpm.sprint5.dwd_dws.service.DwsDowntimeSpringFiveService
- 配置参数：
``` java
"{\"dprunclass\":\"com.foxconn.dpm.sprint5.dwd_dws.controller.DwsDowntimeSpringFiveController\",\"dpmaster\":\"local\",\"dpappName\":\"DwsDowntimeSpringFiveController\",\"pmClass\":\"com.tm.dl.javasdk.dpspark.common.ProdPermissionManager\",\"dpType\":\"scheduling\",\"dpuserid\":\"51736e50-883d-42e0-a484-0633759b8\",\"strategy\":\"WH\",\"workDate\":\"2020-06-09\",\"datasourceType\":\"excel\"}"
```
### dws -> ads
- 启动类：
com.foxconn.dpm.sprint5.dws_ads.controller.AdsDowntimeSpringFiveController
- 业务实现类：
com.foxconn.dpm.sprint5.dws_ads.service.AdsDowntimeSpringFiveService
- 配置参数：
``` java
"{\"dprunclass\":\"com.foxconn.dpm.sprint5.dws_ads.controller.AdsDowntimeSpringFiveController\",\"dpmaster\":\"local\",\"dpappName\":\"AdsDowntimeSpringFiveController\",\"pmClass\":\"com.tm.dl.javasdk.dpspark.common.ProdPermissionManager\",\"dpType\":\"scheduling\",\"dpuserid\":\"51736e50-883d-42e0-a484-0633759b8\",\"strategy\":\"WH\",\"workDate\":\"2020-06-09\"}"
```

---

## 开发模式说明
### Controller 是 spark 任务入口
### Service 是具体的业务实现，可以根据不通的 Site 有多个 Service，通过策略模式绑定 