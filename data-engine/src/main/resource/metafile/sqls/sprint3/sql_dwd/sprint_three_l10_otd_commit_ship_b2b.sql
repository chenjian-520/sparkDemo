SELECT
  Rowkey,
  day_id,
  site_code,
  level_code,
  plant_code,
  customer,
  so_no,
  so_item,
  po_no,
  po_item,
  dn_no,
  customer_pn,
  shiptocountry,
  cast(nvl(sum(ship_qty), 0) AS VARCHAR(32))                                        ship_qty,
  concat(cast(unix_timestamp() AS VARCHAR(32)), '000') update_dt,
  'HS'                                                 update_by,
  'ODS'                                                data_from
FROM
  (
    SELECT
      *
    FROM
      (
        SELECT
          concat(
              '20:',
              concat(cast(to_unix_timestamp(t.day_id, 'yyyy-MM-dd') AS VARCHAR(32)), '000'), ':',
              'WH', ':',
              'L10', ':',
              t.dn_no, ':',
              t.po_no, ':',
              t.po_item, ':',
              'HP'
          )
                                  Rowkey,
          t.day_id,
          t.site_code,
          t.level_code,
          t.plant_code,
          t.customer,
          t.so_no,
          nvl(solineno, 'N/A')    so_item,
          t.po_no,
          t.po_item,
          t.dn_no,
          t.customer_pn,
          nvl(countryname, 'N/A') shiptocountry,
          t.ship_qty
        FROM
          (
            SELECT
              shipdt                                            day_id,
              'WH'                                              site_code,
              'L10'                                             level_code,
              plantcode                                         plant_code,
              'HP'                                              customer,
              sono                                              so_no,
              dpm_ods_production_edidesadvmain.pono             po_no,
              dpm_ods_production_edidesadvdetail.polineno       po_item,
              dpm_ods_production_edidesadvmain.shiporderno      dn_no,
              dpm_ods_production_edidesadvdetail.customerpartno customer_pn,
              nvl(dpm_ods_production_edidesadvdetail.shippedqty,0)     ship_qty
            FROM dpm_ods_production_edidesadvmain, dpm_ods_production_edidesadvdetail
            WHERE dpm_ods_production_edidesadvmain.SHIPDT = '$startDay$' AND
                  dpm_ods_production_edidesadvmain.shiporderno = dpm_ods_production_edidesadvdetail.shiporderno AND
                  dpm_ods_production_edidesadvmain.data_from = 'HPMI'

          ) t
          LEFT JOIN dpm_ods_production_ediordersdetail
            ON t.po_no = dpm_ods_production_ediordersdetail.pono AND
               t.po_item = dpm_ods_production_ediordersdetail.pono
          LEFT JOIN dpm_ods_production_ediordersparty
            ON dpm_ods_production_ediordersparty.partyqualifier = 'ST' AND
               t.po_no = dpm_ods_production_ediordersparty.pono

        UNION ALL

        SELECT
          concat(
              '20:',
              concat(cast(to_unix_timestamp(t.day_id, 'yyyy-MM-dd') AS VARCHAR(32)), '000'), ':',
              'WH', ':',
              'L10', ':',
              t.dn_no, ':',
              t.po_no, ':',
              t.po_item, ':',
              'LENOVO'
          )
                                  Rowkey,
          t.day_id,
          t.site_code,
          t.level_code,
          t.plant_code,
          t.customer,
          t.so_no,
          nvl(solineno, 'N/A')    so_item,
          t.po_no,
          t.po_item,
          t.dn_no,
          t.customer_pn,
          nvl(countryname, 'N/A') shiptocountry,
          t.ship_qty
        FROM (
               SELECT
                 shipdt                                            day_id,
                 'WH'                                              site_code,
                 'L10'                                             level_code,
                 plantcode                                         plant_code,
                 'LENOVO'                                          customer,
                 sono                                              so_no,
                 dpm_ods_production_edidesadvmain.pono             po_no,
                 dpm_ods_production_edidesadvdetail.polineno       po_item,
                 dpm_ods_production_edidesadvmain.shiporderno      dn_no,
                 dpm_ods_production_edidesadvdetail.customerpartno customer_pn,
                 nvl(dpm_ods_production_edidesadvdetail.shippedqty, 0)     ship_qty
               FROM dpm_ods_production_edidesadvmain, dpm_ods_production_edidesadvdetail

               WHERE dpm_ods_production_edidesadvmain.SHIPDT = '$startDay$' AND
                     dpm_ods_production_edidesadvmain.shiporderno = dpm_ods_production_edidesadvdetail.shiporderno AND
                     dpm_ods_production_edidesadvmain.data_from = 'LENOVO'

             ) t
          LEFT JOIN dpm_ods_production_ediordersdetail
            ON t.po_no = dpm_ods_production_ediordersdetail.pono AND
               t.po_item = dpm_ods_production_ediordersdetail.pono
          LEFT JOIN dpm_ods_production_ediordersparty
            ON dpm_ods_production_ediordersparty.partyqualifier = 'ST' AND
               t.po_no = dpm_ods_production_ediordersparty.pono
      )tt
  ) t
GROUP BY
  Rowkey,
  day_id,
  site_code,
  level_code,
  plant_code,
  customer,
  so_no,
  so_item,
  po_no,
  po_item,
  dn_no,
  customer_pn,
  shiptocountry