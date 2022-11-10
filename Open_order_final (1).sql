
-- New Logic (defined by SC on 11/8/22) : 
-- Containers currently in @port or @drayage status: Movement starts 11/14 + 2 week buffer @ 60 container/wk assumption
-- HBL-released containers by fifo (regardless of current @Port or @Drayage status) weeks 1-2
-- Non-HBL-released containers by fifo (regardless of current @Port or @Drayage status) weeks 2-5
-- Shayne Orders:  15 containers/wk (max) starting 11/7 + 2 week buffer (start 21 the last week of november)
-- Standard lead times
-- Non-Shayne Order: Movement Start Date 12/1 + 2 week buffer @ review container flow model for expected container shipments by factory
-- PO Complete by fifo
-- PO Created / PO Confirmed by fifo



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
	  CASE
WHEN dso.magento_sales_order_id ISNULL THEN dpo.magento_sales_order_id 
ELSE  dso.magento_sales_order_id
END AS so_num_co
		,dpo.magento_sales_order_id AS so_num_ns
		,dpo.po_number
		,dpoi.po_line_number
		,dso.order_type
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
		,dso.multifactory
		,tl_poi.location_id
		,loc.address
		,dso.email
		,dso.shipping_first_name
		,dso.shipping_last_name
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
		,CAST(fdsd.delivery_pod_wid AS DATE)
		,DATEDIFF ( 'days', so_date,  CURRENT_DATE) AS so_date_diff
	  ,DATEDIFF ( 'days', so_launch_date,  CURRENT_DATE) AS so_launch_date_diff
  -- Product Level Detail
		,dsoi.cobain_sales_order_item_id AS External_ID
		,dsoi.sku
		,dp.name AS product_description
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
  , (CASE WHEN fp.arrival_port_actual_departure_date='' THEN NULL ELSE fp.arrival_port_actual_departure_date END) AS arrival_port_actual_departure_date1
  , (CASE WHEN fp.destination_actual_arrival_date = '' THEN NULL ELSE fp.destination_actual_arrival_date END) AS destination_actual_arrival_date1
  ,(CASE WHEN fp.house_bill_of_lading_release_date= '' THEN NULL ELSE fp.house_bill_of_lading_release_date END) AS house_bill_of_lading_release_date1
  -- Domestic Shipment Level Data		
		,fdsd.domestic_shipment_wid
		,fdsd.domestic_shipment_details_wid
		,ddsd.sales_order_line_detail_id
		,fdsd.quantity AS domestic_shipment_qty
		--, md.domestic_shipment_name
		--, md.STATUS
  ,ddsd.tracking_number
  ,dso.shipping_zip
 
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
  WHEN (fcs.actual_arrival_port_wid > 0) THEN 'At Arrival Port'

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
   
    WHEN CAST(fp.destination_actual_arrival_date AS DATE) < '1/1/2022' THEN 'closed_order'

  
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
  WHEN carton_status IS NULL THEN 'Exception'
  -- This criteria MUST be the last in the case statement to work properly. Had to back-into the Exceptions because a simple "<" date will include nulls and/or blanks
  WHEN (fp.destination_actual_arrival_date='' OR fp.destination_actual_arrival_date IS NULL) THEN 'OK'
  WHEN CAST(fp.destination_actual_arrival_date AS DATE) < '3/1/2022' THEN 'Exception'
    ELSE 'OK'  
  END AS Exceptions
  
  , CASE 
    WHEN cbm ISNULL THEN tl_poi.est_cbm
    ELSE cbm END AS best_cbm
    
    ,CASE 
    WHEN carton_status = 'At GTZ' THEN 0
    WHEN carton_status = 'At Arrival Port' THEN 1
    WHEN carton_status = 'At Drayage Partner' THEN 2
    WHEN carton_status = 'On Water' THEN 3
    WHEN carton_status = 'PO Complete' THEN 4
    WHEN carton_status IS NULL THEN 6
    ELSE 5
    END AS carton_status_delivery_sort
  
  
    , CASE WHEN dpoi.container_name IN 
    ('EITU1781534'
,'EGHU8471946'
,'EITU1565345'
,'DRYU9177540'
,'KKFU8011144'
,'BMOU5469728'
,'EMCU8252182'
,'FCIU7324001'
,'MAGU5301383'
,'TCNU6369922'
,'EGHU9512248'
,'EITU1158930'
,'EITU9458410'
,'EMCU8294716'
,'EMCU8552385'
,'TGBU7151344'
,'TRHU5180624'
,'TXGU6672783'
,'CBHU9080020'
,'FCIU9119562'
,'OOCU7546988'
,'TGBU4796268'
,'TLLU5612685'
,'NYKU0709021'
,'TLLU4561027'
,'EGHU8487048'
,'MAGU5391469'
,'TCNU6107265'
,'BEAU5532174'
,'NYKU4991191'
,'TCLU7765319'
,'DRYU9498350'
,'FDCU0537448'
,'TCLU8925345'
,'TCNU7573078'
,'BMOU6052960'
,'CMAU7430420'
,'ECMU9574840'
,'SEGU4050502'
,'SEGU6120070'
,'TCLU8365410'
,'YMLU8943719'
,'EGHU9308743'
,'EGHU9476069'
,'EGHU9489528'
,'EGHU9714188'
,'EISU9297013'
,'EITU1618131'
,'EITU1760300'
,'EMCU8714212'
,'EMCU8727190'
,'GAOU6246991'
,'TEMU6137418'
,'TGBU8633720'
,'TRHU6787715'
,'FANU1594177'
,'HLBU1868321'
,'OOLU8995594'
,'SLSU8020088'
,'TCNU8865536'
,'TGBU7178350'
,'TCNU1148868'
,'DRYU9685937'
,'EGHU8475216'
,'EGHU9392604'
,'EITU1375216'
,'EITU1478451'
,'EITU1524095'
,'EITU9106173'
,'EMCU8361239'
,'EMCU8483767'
,'FCIU9982399'
,'TGBU5995813'
,'TGBU6919269'
,'TGBU8629546'
,'TCNU3395506'
,'TCNU8762055'
,'TRHU8688840'
,'DRYU9812348'
,'FBLU0155860'
,'MEDU4815112'
,'MSDU8135506'
,'YMMU6073640'
,'YMMU6230630'
,'YMMU6383990'
,'CSLU6343761'
,'CSNU6995849'
,'FCIU9695486'
,'FFAU3334154'
,'TCLU1541707'
,'BMOU6730780'
,'BSIU9976259'
,'CAAU5049487'
,'CMAU7336223'
,'CMAU9573266'
,'CMAU9576373'
,'CMAU9580394'
,'DRYU9807568'
,'EGHU8515073'
,'EITU1184610'
,'EITU9319081'
,'EMCU8577784'
,'GAOU6605746'
,'OCGU8062660'
,'TCNU3809587'
,'TCNU4335918'
,'TCNU5522261'
,'TGBU4426514'
,'TGBU7033439'
,'TLLU4298116'
,'TRHU8691680'
,'TXGU5579399'
,'WHSU6591756'
,'TCNU6838608'
,'ZCSU6591868'
,'ZCSU6712701'
,'ZCSU7374949'
,'SEKU4424572'
,'TCNU4187488'
,'BEAU5344789'
,'MAGU5513802'
,'NYKU4723521'
,'TCLU8657392'
,'BEAU4834971'
,'FFAU1283567'
,'TCNU1979286'
,'KKFU7551106'
,'KKFU8016887'
,'TCNU5787996'
,'TLLU5518370'
,'CAIU7801451'
,'FFAU3783155'
,'MEDU8923856'
,'MSDU8950150'
,'TXGU5389402'
,'CAIU7995254'
,'YMMU6393664'
,'BSIU9663110'
,'CCLU7437993'
,'CSNU6367591'
,'RFCU5096762'
,'TCNU6978106'
,'ZCSU6718720'
,'ZCSU6737490'
,'ZCSU6760952'
,'ZCSU7247646'
,'ZCSU7524720'
,'ZCSU7719845'
,'ZCSU8472050'
,'BEAU5169109'
,'EGHU9748130'
,'EGSU9055187'
,'EISU9145242'
,'EITU1298526'
,'EITU1431921'
,'EMCU8399677'
,'EMCU9884548'
,'OOLU8744080'
,'TCKU6107996'
,'TCNU1746556'
,'TLLU4844074'
,'DRYU9279542'
,'EGHU9195863'
,'EGHU9780636'
,'EITU9411979'
,'EMCU8360206'
,'EMCU8902858'
,'GAOU6453181'
,'GCXU5005538'
,'TCNU2767337'
,'TCNU3876749'
,'TCNU4767130'
,'TEMU6162555'
,'TEMU7110860'
,'TEMU7541337'
,'TGBU6290900'
,'TRHU7845483'
,'ZCSU6658302'
,'ZCSU6799810'
,'ZCSU7059741'
,'ZCSU7296867'
,'ZCSU7613914'
,'ZCSU7690124'
,'ZCSU7880844'
,'ZCSU7908875'
,'BEAU5529798'
,'BSIU9431500'
,'BSIU9974050'
,'CAAU5193600'
,'CCLU7948859'
,'DRYU9987897'
,'EITU1730706'
,'FCIU9766928'
,'FSCU8047111'
,'MAGU5414754'
,'MAGU5563547'
,'MOTU6731645'
,'NYKU4341616'
,'NYKU4780352'
,'SEGU4469382'
,'SEGU4993316'
,'SEGU5225755'
,'SEGU6859688'
,'TCLU5538661'
,'TCLU6342776'
,'TCNU6552091'
,'TCNU7843445'
,'TGBU5840175'
,'TGBU6083184'
,'TGBU6500930'
,'TLLU4069510'
,'TRHU8254398'
,'TXGU5537032'
,'APHU6692330'
,'FFAU1285847'
,'TCNU7535397'
,'TCNU7912593'
,'CMAU4050940'
,'KKFU7884956'
,'BMOU5244320'
,'BSIU9684935'
,'CAAU5458736'
,'EGHU9778428'
,'EGSU9075706'
,'EISU9191459'
,'EMCU8665210'
,'EMCU8809998'
,'GAOU6103727'
,'IMTU9099396'
,'TEMU8643375'
,'TGBU7896743'
,'TLLU4025976'
,'TLLU4441952'
,'TLLU5206794'
,'TRHU6160638'
,'TXGU5607696'
,'TXGU5763467'
,'EISU9987317'
,'EITU1763253'
,'FCIU9273449'
,'TCNU5980217'
,'BMOU4415964'
,'AXIU1482351'
,'GESU6431925'
,'NYKU4403410'
,'TCLU6697082'
,'TCNU4838872'
,'TCNU4869451'
,'TCNU6711193'
,'TGCU0209788'
,'TRHU5604888'
,'UETU5830717'
,'CMAU9549402'
,'CMAU9551518'
,'CMAU9551884'
,'CMAU9551919'
,'CMAU9552411'
,'CMAU9552520'
,'CMAU9552598'
,'CMAU9611751'
,'GESU6355825'
,'TXGU7307872'
,'BEAU5548777'
,'CMAU7229170'
,'SEKU5547530'
,'TCNU4575009'
,'MEDU8061807'
,'MSMU6094770'
,'TCLU1659460'
,'YMLU8725511'
,'BMOU4146361'
,'BMOU4152180'
,'BSIU9130527'
,'CAIU9739816'
,'GAOU6341710'
,'MEDU7499070'
,'TCKU9848462'
,'TRLU7428718'
,'HMMU6153666'
,'KOCU5020710'
,'CMAU5557375'
,'CMAU8455612'
,'CMAU9383862'
,'CRXU9873293'
,'FCIU8006108'
,'HPCU4336663'
,'TCLU6274162'
,'CAIU9495915'
,'HDMU6814557'
,'HMMU6026152'
,'KOCU4270905'
,'KOCU4991239'
,'NYKU4954351'
,'NYKU5225420'
,'ONEU0212768'
,'TCNU2079756'
,'TGBU6338006'
) THEN 'Restart A'
  WHEN dpo.vendor = 'Shayne' THEN 'Restart B'
  WHEN dpo.vendor = 'Holly Wood' THEN 'Restart C'
  WHEN dpo.vendor = 'Koda' THEN 'Restart C'
  WHEN dpo.vendor = 'Samanni' THEN 'Restart C'
  ELSE 'ERROR'
  END AS restart_path
  
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
  LEFT JOIN fcsd ON dpoi.wid = fcsd.purchase_order_item_wid
  LEFT JOIN datawarehouse.fact_domestic_shipment_details AS fdsd ON fdsd.purchase_order_item_wid = dpoi.wid
  LEFT JOIN datawarehouse.dim_domestic_shipment_details AS ddsd ON fdsd.domestic_shipment_details_wid = ddsd.wid
  LEFT JOIN non_std_ord ON fpoi.purchase_order_wid = non_std_ord.purchase_order_wid
  LEFT JOIN landing_google_sheets.merchandise_hierarchy AS mh ON dsoi.sku = mh.clean_sku
  LEFT JOIN tl_poi ON (dpo.netsuite_purchase_order_id = tl_poi.transaction_id AND dpoi.po_line_number = tl_poi.transaction_line_id)
  LEFT JOIN landing_netsuite.locations AS loc ON tl_poi.location_id = loc.location_id
	LEFT JOIN analysts.flexport as fp ON dpoi.container_name = fp.container_number
	WHERE d.year >= 2020
ORDER BY dpo.po_number
		,dpoi.po_line_number

	)

, re_a AS (
  SELECT 
  com.*
  ,CASE 
    WHEN house_bill_of_lading_release_date1 ISNULL THEN 2
    ELSE 1 END AS hbol
  ,CASE
      WHEN carton_status IN ('PO Complete','PO Confirmed','PO Created') THEN best_cbm
      ELSE 0 END AS unshipped
  , DENSE_RANK() OVER(
    PARTITION BY restart_path
    ORDER BY hbol, arrival_port_actual_arrival_date1
    , container_name) AS con_rank
  , CAST('11/14/2022' AS DATE) AS restart_date
  , 0 AS cbm_con_cap
  , (2*7) AS buffer
  , 0 AS con_cnt
  , CAST((ROUND(con_rank/60,0)+1)*7 AS INT) AS restart_logic
  , CAST((4*7) AS INT) AS remaining_lead_time
  , CAST((restart_date
      + buffer
      + restart_logic
      + remaining_lead_time) AS DATE) AS deliv_date_proto
  , CASE
      WHEN carton_status NOT IN ('PO Complete','PO Confirmed','PO Created') THEN deliv_date_proto
      WHEN CAST((deliv_date_proto-(10*7)) AS DATE) > CAST('1-9-2023' AS DATE) THEN CAST((deliv_date_proto+(3*7)) AS DATE)
      ELSE deliv_date_proto END
      AS exp_deliv_date
  FROM com
  WHERE restart_path = 'Restart A'
  AND open_closed_orders = 'open_order'
  AND vendor != 'JL Jonathan Louis'
  --AND std_index IN ('std','miss_soi_wid')
  AND so_status NOT IN ('cancellation','canceled')
  AND mto_dropship <> 'Dropship'
  AND so_date >= '1/1/2022'
  ORDER BY hbol
    , arrival_port_actual_arrival_date1
    , container_name
) 

, re_b AS (
  SELECT
  com.*
  , 0 AS hbol
  ,CASE
      WHEN carton_status IN ('PO Complete','PO Confirmed','PO Created') THEN best_cbm
      ELSE 0 END AS unshipped
  ,(SUM(unshipped) OVER (
    PARTITION BY restart_path
    ORDER BY 
    carton_status_delivery_sort
    ,act_factory_finish_date
    ,so_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS con_rank
  , CURRENT_DATE AS restart_date
  , 21 AS cbm_con_cap
  , (2*7) AS buffer
  , CAST(ROUND((con_rank+30)/60,0) AS INT) AS con_cnt
  , CASE
  WHEN carton_status IN ('PO Complete','PO Confirmed','PO Created') THEN
  CAST(ROUND((con_cnt/cbm_con_cap)+1,0) AS INT)*7 ELSE 0 END AS restart_logic
  , CASE
      WHEN carton_status IN ('PO Complete','PO Confirmed','PO Created') THEN CAST(12*7 AS INT)
      WHEN carton_status IN ('On Water') THEN CAST(11*7 AS INT)
      WHEN carton_status IN ('At Arrival Port') THEN CAST(7*7 AS INT)
      WHEN carton_status IN ('At Drayage Partner') THEN CAST(6*7 AS INT)
      WHEN carton_status IN ('At GTZ') THEN CAST(5*7 AS INT)
      END AS remaining_lead_time
  , CAST((restart_date
      + buffer
      + restart_logic
      + remaining_lead_time) AS DATE) AS deliv_date_proto
  , CASE
      WHEN carton_status NOT IN ('PO Complete','PO Confirmed','PO Created') THEN deliv_date_proto
      WHEN CAST((deliv_date_proto-(10*7)) AS DATE) > CAST('1-9-2023' AS DATE) THEN CAST((deliv_date_proto+(3*7)) AS DATE)
      ELSE deliv_date_proto END
      AS exp_deliv_date
  FROM com
  WHERE restart_path = 'Restart B'
  AND open_closed_orders = 'open_order'
  AND vendor != 'JL Jonathan Louis'
  --AND std_index IN ('std','miss_soi_wid')
  AND so_status NOT IN ('cancellation','canceled')
  AND mto_dropship <> 'Dropship'
  AND so_date >= '1/1/2022'
  ORDER BY carton_status_delivery_sort
    ,act_factory_finish_date
    ,so_date

) 


, re_c AS (
  SELECT 
  com.*
  ,0 AS hbol
  ,CASE
      WHEN carton_status IN ('PO Complete','PO Confirmed','PO Created') THEN best_cbm
      ELSE 0 END AS unshipped
  ,(SUM(unshipped) OVER (
    PARTITION BY vendor,restart_path
    ORDER BY 
    carton_status_delivery_sort
    ,act_factory_finish_date
    ,so_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS con_rank

  , CASE 
    WHEN CURRENT_DATE <= '12/1/2022' THEN CAST('12/1/2022' AS DATE)
    ELSE CURRENT_DATE
    END AS restart_date
  , CASE
    WHEN vendor = 'Holly Wood' THEN 5
    WHEN vendor = 'Samanni' THEN 20
    WHEN vendor = 'Koda' THEN 3
    ELSE 0 END AS cbm_con_cap
  , (2*7) AS buffer
  , CAST(ROUND((con_rank+30)/60,0) AS INT) AS con_cnt
  , CASE
  WHEN carton_status IN ('PO Complete','PO Confirmed','PO Created') THEN
  CAST(ROUND((con_cnt/cbm_con_cap)+1,0) AS INT)*7 ELSE 0 END AS restart_logic
  , CASE
      WHEN carton_status IN ('PO Complete','PO Confirmed','PO Created') THEN CAST(12*7 AS INT)
      WHEN carton_status IN ('On Water') THEN CAST(11*7 AS INT)
      WHEN carton_status IN ('At Arrival Port') THEN CAST(7*7 AS INT)
      WHEN carton_status IN ('At Drayage Partner') THEN CAST(6*7 AS INT)
      WHEN carton_status IN ('At GTZ') THEN CAST(5*7 AS INT)
      END AS remaining_lead_time
  , CAST((restart_date
      + buffer
      + restart_logic
      + remaining_lead_time) AS DATE) AS deliv_date_proto
  , CASE
      WHEN carton_status NOT IN ('PO Complete','PO Confirmed','PO Created') THEN deliv_date_proto
      WHEN CAST((deliv_date_proto-(10*7)) AS DATE) > CAST('1-9-2023' AS DATE) THEN CAST((deliv_date_proto+(3*7)) AS DATE)
      ELSE deliv_date_proto END
      AS exp_deliv_date
  FROM com
  WHERE restart_path = 'Restart C'
  AND open_closed_orders = 'open_order'
  AND vendor != 'JL Jonathan Louis'
  --AND std_index IN ('std','miss_soi_wid')
  AND so_status NOT IN ('cancellation','canceled')
  AND mto_dropship <> 'Dropship'
  AND so_date >= '1/1/2022'
  ORDER BY 
    vendor
    ,carton_status_delivery_sort
    ,act_factory_finish_date
    ,so_date

) 
, restart_query AS (
SELECT * FROM re_a 
UNION ALL
SELECT * FROM re_b 
UNION ALL 
SELECT * FROM re_c )

SELECT *
FROM
restart_query


