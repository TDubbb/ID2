WITH tl_poi
AS (
	SELECT 
	  tl.transaction_id
	  ,tl.transaction_line_id
	  ,tl.transaction_id || '.' || tl.transaction_line_id AS tl_poi_wid
		,tl.act_factory_finish_date
		,tl.confirmation_date
		,tl.act_loading_date
		,tl.carton_height
		,tl.carton_length
		,tl.carton_width
		,tl.est_cbm
		,tl.location_id
        ,tl.quantity_received_in_shipment
		,tl.shipment_received
	FROM landing_netsuite.transaction_lines AS tl
	INNER JOIN landing_netsuite.transactions AS t ON tl.transaction_id = t.transaction_id
	WHERE t.transaction_type = 'Purchase Order'
		AND tl.transaction_line_id <> 0
	)
	,non_std_ord
AS (
	SELECT fpoi.purchase_order_wid
		,CASE 
			WHEN dpo.magento_sales_order_id LIKE '9%'
				THEN 'non_std'
			WHEN dpo.magento_sales_order_id ISNULL
				THEN 'non_std'
			WHEN dpo.magento_sales_order_id LIKE 'H%'
				THEN 'non_std'
			-- MIssinig SOI WID / External Item ID but it is not a Studio Order
			WHEN dpo.magento_sales_order_id LIKE '1%'
				THEN 'miss_soi_wid'
			ELSE LEFT(dpo.magento_sales_order_id, 1)
			END AS std_index
	FROM datawarehouse.fact_purchase_order_items AS fpoi
	INNER JOIN datawarehouse.dim_date AS d ON fpoi.created_at_wid = d.wid
	INNER JOIN datawarehouse.dim_purchase_orders AS dpo ON fpoi.purchase_order_wid = dpo.wid
		WHERE d.year >= 2020
		AND fpoi.sales_order_items_wid ISNULL
	GROUP BY 1
		,2
	)
	, 
	max_delivery as (
	SELECT * FROM (
	SELECT dso.wid as so_wid 
  	, dds.domestic_shipment_id
		, dds.domestic_shipment_name
		, dds.STATUS
	  , fds.delivery_pod_wid
	  , d.date 
	  , row_number() over (
	    PARTITION BY dso.wid 
	    ORDER BY d.date DESC
	    ) as order_sequence
	  
	  FROM datawarehouse.dim_sales_orders as dso 
      LEFT JOIN datawarehouse.fact_domestic_shipments AS fds ON dso.wid = fds.order_wid
      LEFT JOIN datawarehouse.dim_domestic_shipments AS dds ON fds.domestic_shipment_wid = dds.wid
      LEFT JOIN datawarehouse.dim_date as d on cast(fds.delivery_pod_wid as date) = d.date
      
      
      --where d.year > 2022 and d.month = 4
      ORDER BY dso.wid, order_sequence
  )
    where order_sequence = 1
  ) 

, poi AS (
  SELECT
    fpoi.purchase_order_item_wid AS wid,
    SUM(fpoi.quantity) AS po_units
  FROM
    datawarehouse.fact_purchase_order_items AS fpoi
  GROUP BY 1
),
fcs AS (
  SELECT fcsd.purchase_order_item_wid AS wid
    , SUM(fcsd.quantity) AS fc_units
  FROM datawarehouse.fact_container_shipment_details AS fcsd
  GROUP BY 1
), 
fcs_fil AS (
SELECT poi.wid
  , poi.po_units
  , fcs.fc_units
  , (poi.po_units = fcs.fc_units) AS t_f
  , fcs.fc_units - poi.po_units AS diff
FROM 
  poi 
  INNER JOIN fcs ON poi.wid = fcs.wid
WHERE t_f = False
ORDER BY diff
),
fcsd AS (
  SELECT 
    fcsd.purchase_order_item_wid
    , sum(fcsd.unit_cbm) AS ext_cbm
  FROM datawarehouse.fact_container_shipment_details AS fcsd
  WHERE fcsd.purchase_order_item_wid NOT IN (
                                            SELECT wid
                                            FROM fcs_fil
  )
  GROUP BY 1
)


	,com
AS (
	SELECT 
	-- Purchase Order Level Details
	  dso.magento_sales_order_id AS so_num_co
		,dpo.magento_sales_order_id AS so_num_ns
		,dpo.po_number
		,dpoi.po_line_number
		,dso.status AS SO_Status
		,dpo.status AS PO_Status
		,dpo.factory_status
		,dsoi.fulfillment_classification
		,CASE 
			WHEN non_std_ord.std_index ISNULL
				THEN 'std'
			ELSE non_std_ord.std_index
			END AS std_index
		,dpo.vendor
		,dsoi.bndl
		,tl_poi.location_id
		,loc.address
		,dso.email
		-- Dates for comparison
		,cast(fso.created_at_wid_ct AS DATE) AS so_date		
		,cast(fsoi.bndl_fabric_selected_wid AS DATE) AS bndl_date		
		,COALESCE(bndl_date, so_date) AS so_launch_date 
		,CAST(d.wid AS DATE) AS po_date		
		,CAST(fcs.actual_arrival_port_wid AS DATE) AS actual_arrival_port_date
		,CAST(fcs.actual_arrival_wh_wid AS DATE) AS actual_arrival_wh_date		
		,tl_poi.act_factory_finish_date
		,CAST(fcs.actual_departure_wid AS DATE) AS actual_departure_date
		,tl_poi.confirmation_date
		,tl_poi.act_loading_date
		,CAST(md.delivery_pod_wid AS DATE)
		, DATEDIFF ( 'days', so_launch_date,  CURRENT_DATE) AS date_diff
	 
  -- Product Level Detail
		,dsoi.sku
		,mh.division
		,mh.class
		,mh.sub_class
		,mh.micro_class
		,mh.nano_class
		,mh.pico_class
		,dp.collection
		,mh.mto_dropship
		
-- Component level measures		
		,dpoi.piece_description
		,fpoi.rate
		,fpoi.quantity AS comp_qty
		,fpoi.total_amount
		,fcsd.ext_cbm AS cbm
		,tl_poi.carton_height
		,tl_poi.carton_length
		,tl_poi.carton_width
		,tl_poi.est_cbm
		,tl_poi.quantity_received_in_shipment
		,tl_poi.shipment_received
-- Container Level Data
		,dpoi.container_name
		,dcs.container_tracking_id
	, (CASE WHEN fp.container_number = '' THEN NULL ELSE fp.container_number END) AS flexport_container_number	
  , (CASE WHEN fp.departure_port_actual_departure_date = '' THEN NULL ELSE fp.departure_port_actual_departure_date END) AS departure_port_actual_departure_date1
  , (CASE WHEN fp.arrival_port_actual_arrival_date = '' THEN NULL ELSE fp.arrival_port_actual_arrival_date END) AS arrival_port_actual_arrival_date1
  , (CASE WHEN fp.arrival_port_actual_departure_date = '' THEN '1-1-1900' ELSE fp.arrival_port_actual_departure_date END) AS arrival_port_actual_departure_date1
  , (CASE WHEN fp.destination_actual_arrival_date = '' THEN NULL ELSE fp.destination_actual_arrival_date END) AS destination_actual_arrival_date1
  ,(CASE WHEN fp.house_bill_of_lading_release_date= '' THEN NULL ELSE fp.house_bill_of_lading_release_date END) AS house_bill_of_lading_release_date1
  -- Domestic Shipment Level Data		
		, md.domestic_shipment_id
		, md.domestic_shipment_name
		, md.STATUS
		,lds.order_complete_status_id
  , dso.shipping_zip
  ,dso.shipping_first_name
,dso.shipping_last_name
,dp.name AS product_description
  , CASE
-- IF([so_status]="delivered" OR ISNULL([delivery_pod_wid])=false) THEN "Delivered"
    WHEN (so_status ='delivered' OR delivery_pod_wid > 0) THEN 'Delivered'

-- ELSEIF(ISNULL([Destination Actual Arrival Date])=false AND ISNULL([delivery_pod_wid])) THEN "At GTZ"IS NOT NULL
    WHEN (fp.destination_actual_arrival_date > 0  AND delivery_pod_wid IS NULL ) THEN 'At GTZ'
  WHEN (fcs.actual_arrival_wh_wid > 0  AND delivery_pod_wid IS NULL ) THEN 'At GTZ'
    
-- ELSEIF(ISNULL([Arrival Port Actual Departure Date])=false AND ISNULL([Flexport Container Number])=false AND ISNULL([Destination Actual Arrival Date])) THEN "At Drayage Partner"
  WHEN (fp.arrival_port_actual_departure_date > 0 AND (fp.destination_actual_arrival_date = '' OR fp.destination_actual_arrival_date IS NULL)) THEN 'At Drayage Partner'
  WHEN (fp.arrival_port_actual_departure_date > 0 AND fcs.actual_arrival_wh_wid IS NULL) THEN 'At Drayage Partner'
  
-- ELSEIF(ISNULL([Arrival Port Actual Arrival Date])=false AND ISNULL([Flexport Container Number])=false AND ISNULL([Arrival Port Actual Departure Date])) THEN "At Arrival Port"
  WHEN (fp.arrival_port_actual_arrival_date > 0 AND (fp.arrival_port_actual_departure_date='' OR fp.arrival_port_actual_departure_date IS NULL)) THEN 'At Arrival Port'
  WHEN (fcs.actual_arrival_port_wid > 0) THEN 'At Arrival Port 2'

-- ELSEIF(ISNULL([Departure Port Actual Departure Date])=false AND ISNULL([Arrival Port Actual Arrival Date])) THEN "On Water"
  WHEN (fp.departure_port_actual_departure_date > 0 AND (fp.arrival_port_actual_arrival_date='' OR fp.arrival_port_actual_arrival_date IS NULL) ) THEN 'On Water'
  WHEN (fcs.actual_departure_wid > 0 AND fcs.actual_arrival_port_wid IS NULL) THEN 'On Water'

-- ELSEIF(ISNULL([Actual Factory Finish Date])=false AND ISNULL([Departure Port Actual Departure Date])) THEN "PO Complete"
  WHEN (tl_poi.act_factory_finish_date > 0 AND (fp.departure_port_actual_departure_date='' OR fp.departure_port_actual_departure_date IS NULL)) THEN 'PO Complete'
  WHEN (tl_poi.act_factory_finish_date > 0 AND fcs.actual_departure_wid IS NULL) THEN 'PO Complete'

-- ELSEIF(ISNULL([PO Confirmation Date])=false AND [factory_status]<>"Closed" AND ISNULL([Actual Factory Finish Date])) THEN "PO Confirmed"
  WHEN (tl_poi.confirmation_date > 0 AND dpo.factory_status <>'Closed' AND tl_poi.act_factory_finish_date IS NULL) THEN 'PO Confirmed'

-- ELSEIF (ISNULL([PO Confirmation Date]) AND [factory_status]="Processing") THEN "PO Created"
  WHEN (tl_poi.confirmation_date IS NULL AND dpo.factory_status <>'Closed' AND dpo.factory_status <>'Cancellation') THEN 'PO Created'
  
-- END
  END AS carton_status
  
  
  
  ,CASE
    WHEN carton_status = 'Delivered' THEN 'closed_order'
    WHEN factory_status = 'Cancellation' THEN 'closed_order'
    WHEN dpoi.container_name = 'CAAU5853556' THEN 'closed_order'
    WHEN dpo.po_number IN 
        (
        'SA69446ID'
        ,'SA96253ID'
        ,'HO109971ID'
        ,'HO113743ID'
        ,'SH119071ID'
        ,'SH119788ID'
        ,'SH113928ID'
        ,'SH115711ID'
        ,'100073941'
        ,'SA63706ID-1'
        )  THEN 'closed_order'
        
-- df = df[(df['so_date'] >= datetime.datetime(2020,1,1)) | (df['so_date'].isnull())]
   WHEN so_date >= '1/1/2020' THEN 'open_order'

-- df = df[(df['so_launch_date'] >= datetime.datetime(2021,1,1)) | (df['so_launch_date'].isnull())]
  WHEN so_launch_date >= '1/1/2021' THEN 'open_order'

-- df = df[(df['destination_actual_arrival_date'] >= datetime.datetime(2022,1,1)) | (df['destination_actual_arrival_date'].isnull())]
   WHEN (fp.destination_actual_arrival_date='' OR fp.destination_actual_arrival_date IS NULL) THEN 'open_order'
   
    WHEN fp.destination_actual_arrival_date < '1/1/2022' THEN 'closed_order'

  
    ELSE 'open_order'  
  END AS open_closed_orders
  
   ,CASE
    WHEN carton_status = 'Delivered' THEN 'OK'
    WHEN factory_status = 'Cancellation' THEN 'OK'
    -- when factory status <> canecellation but SO status = cancellation then quickship?/ owned inventory? what if only partial units cancelled on PO / SO?
    WHEN dpoi.container_name = 'CAAU5853556' THEN 'Exception'
    WHEN dpo.po_number IN 
        (
        'SA69446ID'
        ,'SA96253ID'
        ,'HO109971ID'
        ,'HO113743ID'
        ,'SH119071ID'
        ,'SH119788ID'
        ,'SH113928ID'
        ,'SH115711ID'
        ,'100073941'
        ,'SA63706ID-1'
        )  THEN 'Exception'
        
    
   WHEN so_date < '1/1/2020' THEN 'Exception'
  WHEN so_launch_date < '1/1/2021' THEN 'Exception'
  WHEN std_index IN ('miss_soi_wid') THEN 'Exception'
  -- This criteria MUST be the last in the case statement to work properly. Had to back-into the Exceptions because a simple "<" date will include nulls and/or blanks
  WHEN (fp.destination_actual_arrival_date='' OR fp.destination_actual_arrival_date IS NULL) THEN 'OK'
  WHEN fp.destination_actual_arrival_date < '1/1/2022' THEN 'Exception'


    ELSE 'OK'  
  END AS Exceptions
  
  , CASE 
    WHEN cbm ISNULL THEN tl_poi.est_cbm
    ELSE bm END AS best_cbm
  
FROM datawarehouse.fact_purchase_order_items AS fpoi
  INNER JOIN datawarehouse.dim_purchase_order_items AS dpoi ON fpoi.purchase_order_item_wid = dpoi.wid
  INNER JOIN datawarehouse.fact_purchase_orders AS fpo ON fpoi.purchase_order_wid = fpo.purchase_order_wid
  INNER JOIN datawarehouse.dim_purchase_orders AS dpo ON fpo.purchase_order_wid = dpo.wid
  INNER JOIN datawarehouse.dim_date AS d ON fpo.po_created_date_wid = d.wid
  LEFT JOIN datawarehouse.dim_sales_orders AS dso ON fpo.order_wid = dso.wid
  LEFT JOIN datawarehouse.dim_sales_order_items AS dsoi ON fpoi.sales_order_items_wid = dsoi.wid
  LEFT JOIN datawarehouse.fact_sales_orders AS fso ON fpo.order_wid = fso.order_wid
  LEFT JOIN datawarehouse.fact_sales_order_items AS fsoi ON fpoi.sales_order_items_wid = fsoi.sales_order_items_wid
  LEFT JOIN datawarehouse.dim_products AS dp ON fsoi.product_wid = dp.wid
  LEFT JOIN datawarehouse.dim_container_shipments AS dcs ON dcs.container_name = dpoi.container_name
  LEFT JOIN datawarehouse.fact_container_shipments AS fcs ON fcs.container_shipment_wid = dcs.wid
  LEFT JOIN max_delivery AS md ON dso.wid = md.so_wid
  LEFT JOIN fcsd ON dpoi.wid = fcsd.purchase_order_item_wid
--LEFT JOIN datawarehouse.dim_domestic_shipments AS dds ON fds.domestic_shipment_wid = dds.wid
  LEFT JOIN landing_netsuite.domestic_shipment AS lds ON lds.domestic_shipment_id = md.domestic_shipment_id
  LEFT JOIN non_std_ord ON fpoi.purchase_order_wid = non_std_ord.purchase_order_wid
  LEFT JOIN landing_google_sheets.merchandise_hierarchy AS mh ON dsoi.sku = mh.clean_sku
  LEFT JOIN tl_poi ON (dpo.netsuite_purchase_order_id = tl_poi.transaction_id AND dpoi.po_line_number = tl_poi.transaction_line_id)
  LEFT JOIN landing_netsuite.locations AS loc ON tl_poi.location_id = loc.location_id
	LEFT JOIN analysts.flexport as fp ON dpoi.container_name = fp.container_number
	WHERE d.year >= 2020
ORDER BY dpo.po_number
		,dpoi.po_line_number

	)

SELECT   destination_actual_arrival_date1
  ,actual_arrival_wh_date
  ,arrival_port_actual_departure_date1 -- NULL FIRST
  ,arrival_port_actual_arrival_date1
  ,actual_arrival_port_date
  ,departure_port_actual_departure_date1
  ,actual_departure_date
  ,act_factory_finish_date
  ,so_launch_date
  ,best_cbm
FROM com
WHERE
  open_closed_orders = 'open_order'
  AND vendor != 'JL Jonathan Louis'
  AND std_index = 'std'
  AND so_status NOT IN ('cancellation','canceled')
  AND mto_dropship = 'MTO'
ORDER BY
  destination_actual_arrival_date1
  ,actual_arrival_wh_date
  ,arrival_port_actual_departure_date1 -- NULL FIRST by hard coding 1/1/1900 First
  ,arrival_port_actual_arrival_date1
  ,actual_arrival_port_date
  ,departure_port_actual_departure_date1
  ,actual_departure_date
  ,act_factory_finish_date
  ,so_launch_date
