SELECT
  Rowkey,
  day_id,
  site_code,
  level_code,
  plant_code,
  customer,
  so_no,
  nvl(so_item, 'N/A') so_item,
  po_no,
  po_item,
  dn_no,
  dn_item,
  customer_pn,
  nvl(shiptocountry, 'N/A') shiptocountry,
  cast(nvl(sum(commit_ship_qty), 0) AS VARCHAR(32)) commit_ship_qty,
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
                concat(cast(to_unix_timestamp(actualshipdate, 'yyyy-MM-dd') AS VARCHAR(32)), '000'), ':',
                'WH', ':',
                'L10', ':',
                'N/A', ':',
                'N/A', ':',
                hp_po, ':',
                dpm_ods_production_scm_hp_commit_ship.hp_po_line_item, ':',
                'HP'
            )
                                       Rowkey,
            actualshipdate             day_id,
            'WH'                       site_code,
            'L10'                      level_code,
            owner                      plant_code,
            'HP'                       customer,
            hp_so                      so_no,
            hp_so_line_item            so_item,
            hp_po                      po_no,
            hp_po_line_item            po_item,
            'N/A'                      dn_no,
            'N/A'                      dn_item,
            hp_pn                      customer_pn,
            ship_to_country_code       shiptocountry,
            nvl(pack_id_line_item_unit_qty, 0) commit_ship_qty
          FROM dpm_ods_production_scm_hp_commit_ship
          WHERE actualshipdate = '$startDay$'
          UNION ALL
          SELECT
            concat(
                '20:',
                concat(cast(to_unix_timestamp(shipdate, 'yyyy-MM-dd') AS VARCHAR(32)), '000'), ':',
                'WH', ':',
                'L10', ':',
                hfjdn, ':',
                hfjdnitem, ':',
                customerpo, ':',
                customerpoitem, ':',
                'LENOVO'
            )
                           Rowkey,
            shipdate       day_id,
            'WH'           site_code,
            'L10'          level_code,
            plantcode      plant_code,
            'LENOVO'       customer,
            hfjso          so_no,
            'N/A'          so_item,
            customerpo     po_no,
            customerpoitem po_item,
            hfjdn          dn_no,
            hfjdnitem      dn_item,
            partno         customer_pn,
            shiptocountry  shiptocountry,
            nvl(dnqty, 0)          commit_ship_qty
          FROM dpm_ods_production_scm_lenovo_commit_ship
          WHERE shipdate = '$startDay$'
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
  dn_item,
  customer_pn,
  shiptocountry