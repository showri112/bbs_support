import json
import sys
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import  StringType, MapType
from pyspark.sql.functions import udf
from awsglue.transforms import *


def scan_response(data):
    try:
        final_data = {}

        bag_resp = data.get("bag_resp", {})
        hdr = bag_resp.get("hdr", {})
        scanner_info = hdr.get("scanner_info", {})
        scanner_info_columns = ["dev_id", "mac_id", "ver", "serial_nbr"]
        for col in scanner_info_columns:
            final_data[col] = scanner_info.get(col)
        
        hdr_columns = ["action_cd", "stn_cd"]
        for col in hdr_columns:
            final_data[col] = hdr.get(col)

        final_data["ret_code"] = bag_resp.get("ret_code")
        final_data["error_msg"] = bag_resp.get("error_msg")

        resp_info = bag_resp.get("resp_info", [{}])[0]
        pax_info = resp_info.get("pax_info", {})
        flt_info = resp_info.get("flt_info", [{}])[0]

        pax_info_columns = ["fn", "ln", "pax_stat"]
        for col in pax_info_columns:
            final_data[f"pax_info_{col}"] = pax_info.get(col)

        flight_info_columns = ["orig", "dest", "gate", "flt_nbr", "carr_cd", "lcl_est_arr_dtm", "lcl_est_dep_dtm", "lcl_sch_arr_dtm", 
                            "lcl_sch_dep_dtm", "flt_status", "nose_nbr", "equip_cd", "acft_nbr", "TailNumber", "flt_type",
                            "wideBdyInd","arrBagClaimUnitId","eqpDesc","orig_flt_dt","sector_cd","isRDLFlt","isTailTip","wgtRstInd",
                            "opsLegDepOccurNbr","opsLegArrOccurNbr","starFltInd","priorityFltInd","pier","bag_status","scan_dtm","seq_nbr",
                            "tag_nbr","type","priority_bag","bag_primary_type","curr_sector","orig_sector","is_animal_ok","is_crate_ok"]

        for col in flight_info_columns:
            final_data[f"flt_info_{col}"] = flt_info.get(col)

        bag_tag = resp_info.get("bag_tag",[{}])[0]
        bag_tag_columns = ["bag_status","scan_dtm","seq_nbr","tag_nbr","type","priority_bag","bag_primary_type","curr_sector","orig_sector","is_animal_ok","is_crate_ok"]
        for col in bag_tag_columns:
            final_data[f"bag_tag_{col}"] = bag_tag.get(col)

        last_action_columns = ["remark", "seq_nbr", "storage_nbr", "stage_nbr", "flt_nbr", "carr_cd", "stn"]
        last_action = bag_tag.get("last_action", {})
        for col in last_action_columns:
            final_data[f"last_action_{col}"] = last_action.get(col)

        clm_info_columns = ["clm_svc_ret_code", "clm_svc_ret_msg", "bolt_ref_nbr", "wt_ref_nbr"]
        clm_info = resp_info.get("clm_info", {})
        for col in clm_info_columns:
            final_data[f"clm_info_{col}"] = clm_info.get(col)
        

        curr_itnr_columns = ["flt_nbr", "carr_cd", "orig", "dest", "lcl_est_arr_dtm", "lcl_est_dep_dtm", "lcl_sch_arr_dtm", 
                            "lcl_sch_dep_dtm", "gate", "intl_ind", "flt_status", "arrBagClaimUnitId", "sector_cd"]
        current_inin = resp_info.get('curr_itin',{}).get("flt_info", [{}])

        for column in curr_itnr_columns:
            for index in range(5):
                final_data[f"resp_curritnr_{index}_{column}"] = (
                    current_inin[index].get(column) if len(current_inin) > index else None
                )
        origi_itnr_columns = ["flt_nbr", "carr_cd", "orig", "dest", "lcl_est_arr_dtm", "lcl_est_dep_dtm", "lcl_sch_arr_dtm", 
                            "lcl_sch_dep_dtm", "gate", "intl_ind", "flt_status", "arrBagClaimUnitId", "sector_cd"]
        orig_itin = resp_info.get('orig_itin',{}).get("flt_info", [{}])
        for column in origi_itnr_columns:
            for index in range(5):
                final_data[f"resp_origitnr_{index}_{column}"] = (
                    orig_itin[index].get(column) if len(orig_itin) > index else None
                )
        flt_avlb = resp_info.get('flt_avlb_info',{}).get("flt_avlb",{})
        final_data['flt_svc_ret_code'] = flt_avlb.get('flt_svc_ret_code')
        final_data['flt_svc_ret_msg'] = flt_avlb.get('flt_svc_ret_msg')
        flt_avlb_flt_info = flt_avlb.get('flt_info',{})
        flt_avlb_flt_info_columns = ["flt_nbr", "carr_cd", "orig", "dest", "lcl_est_arr_dtm", "lcl_est_dep_dtm", "lcl_sch_arr_dtm",
                                    "gate", "intl_ind", "equip_cd", "flt_status"]
        for col in flt_avlb_flt_info_columns:
            final_data[f"flt_avlb_flt_info_{col}"] = flt_avlb_flt_info.get(col)
        resp_info_columns = ["storage_loc", "prnt_ret_code", "prnt_ret_msg", "prnt_error_msg", "fnl_dest_stn_cd", "int_ind", "oa_conct_ind"]
        
        for col in resp_info_columns:
            final_data[f"resp_info_{col}"] = resp_info.get(col)
        

        print_parm = resp_info.get('print_parm',{})
        print_parm_columns = ["print_type", "print_addr"]
        for col in print_parm_columns:
            final_data[col] = print_parm.get(col)
        
        bag_count = resp_info.get('bag_count',{})
        bag_count_columns = ["heavyBags", "total_bag_cnt", "total_scan_cnt", "standBags", "missingBags", "quickPaks", "valetBags", "min_to_dep"]
        for col in bag_count_columns:
            final_data[col] = bag_count.get(col)

        msng_bag_sum_by_sector = resp_info.get('msng_bag_sum_by_sector',{})
        msng_bag_sum_by_sector_columns = ["total_prio_bags", "total_city_bags", "total_other_bags"]
        for col in msng_bag_sum_by_sector_columns:
            final_data[col] = msng_bag_sum_by_sector.get(col)
        
        tb_by_sector = msng_bag_sum_by_sector.get('total_tb_bags',{}).get("tb_by_sector",{})
        tb_by_sector_columns = ["count", "sector"]
        for col in tb_by_sector_columns:
            final_data[f"tb_by_sector_{col}"] = tb_by_sector.get(col)
        
        br_trans_res_pax_info = resp_info.get('br_trans_res',{}).get("pax_info",{})
        br_trans_res_pax_info_columns = ["fn", "ln", "bag_status", "scan_dtm", "tag_nbr", "content_cd", "priority_bag", "bag_primary_type", "bag_secondary_type", "curr_sector", "orig_sector"]
        for col in br_trans_res_pax_info_columns:
            final_data[f"br_trans_res_pax_info_{col}"] = br_trans_res_pax_info.get(col)
        
        br_trans_res_flt_info = resp_info.get('br_trans_res',{}).get("scanned_flt_info",{}).get("flt_info ",{})
        br_trans_res_flt_info_columns = ["flt_nbr", "carr_cd", "orig", "dest", "lcl_est_arr_dtm", "lcl_est_dep_dtm", "lcl_sch_arr_dtm", "lcl_sch_dep_dtm", "gate", "intl_ind",
                                        "acft_nbr", "TailNumber", "equip_cd", "flt_status", "wideBdyInd", "eqpDesc", "orig_flt_dt", "sector_cd", "isRDLFlt", "isTailTip", 
                                        "wgtRstInd", "isCurrent", "isActive", "psgr_status", "auth_to_load", "csr_auth_to_load"]
                                        
        for col in br_trans_res_flt_info_columns:
            final_data[f"br_trans_res_flt_info_{col}"] = br_trans_res_flt_info.get(col)
        
        
        br_trans_batch_res = resp_info.get('br_trans_batch_res',{}).get("bag_tag",{})
        br_trans_batch_res_pax_info = br_trans_batch_res.get("pax_info",{})
        br_trans_batch_res_pax_info_columns = ["fn", "ln", "tag_nbr", "content_cd", "priority_bag", "bag_primary_type", "bag_secondary_type"]
        for col in br_trans_batch_res_pax_info_columns:
            final_data[f"br_trans_batch_res_pax_info_{col}"] = br_trans_batch_res_pax_info.get(col)
            
        br_trans_batch_res_flt_info = br_trans_batch_res.get("scanned_flt_info",{}).get("flt_info",{})
        br_trans_batch_res_flt_info_columns = ["flt_nbr", "carr_cd", "orig", "dest", "lcl_est_arr_dtm", "lcl_est_dep_dtm", "lcl_sch_arr_dtm", "lcl_sch_dep_dtm", "gate", "intl_ind",
                                            "acft_nbr", "nose_nbr", "TailNumber", "equip_cd", "flt_status", "wideBdyInd", "eqpDesc", "sector_cd", "isRDLFlt", "isTailTip",
                                            "wgtRstInd", "isCurrent", "isActive", "isOriginal", "starFltInd", "priorityFltInd", "psgr_status", "auth_to_load", "csr_auth_to_load", "legSeqNbr"]
        
        for col in br_trans_batch_res_flt_info_columns:
            final_data[f"br_trans_batch_res_flt_info_{col}"] = br_trans_batch_res_flt_info.get(col)
        
        return final_data
    except Exception as e:
        return None
    


def bm_response(json_data):
    try:
        final_data = {}

        bag_resp = json_data.get("bagResp", {})
        hdr = bag_resp.get("hdr", {})
        bag_resp_info = bag_resp.get("bagRespInfo", {})
        flight_info = bag_resp_info.get("flightInfo", {})
        bag_tag = bag_resp_info.get("bagTag", {})

        scanner_info = hdr.get("scannerInformation", {})
        scanner_info_columns = ["scannerMACAddress", "scannerID", "scannerSerialNumber", "version"]
        for col in scanner_info_columns:
            final_data[col] = scanner_info.get(col)

        final_data["storageLocation"] = bag_resp_info.get("storageLocation")
        hdr_columns = ["correlationID", "scanActionCode", "actionStation", "userID","transactionID" ]
        for col in hdr_columns:
            final_data[col] = hdr.get(col)

        final_data["responseCode"] = bag_resp.get("responseCode")
        final_data["responseMessage"] = bag_resp.get("responseMessage")
        final_data["bagKey"] = bag_resp.get("bagKey")

        pax = bag_resp_info.get("pax", {})
        pax_columns = ["firstName", "lastName", "paxStat"]
        for col in pax_columns:
            final_data[col] = pax.get(col)

        bagRespInfo_flt_info_columns = ["arrivalStation", "carrierCode","flightNumber","departureDate", "flightStatus","departureGate", "localEstimatedDepartureDate",
            "localEstimateArrivalDate", "localScheduledArrivalDate", "localScheduledDepartureDate", "departureBagSectorNumber", "isInternational", "acftNumber",
            "isRDLFlight", "tailTipIndicator", "equipmentCode", "freezeWeightIndicator", "tailNumber", "carrierType", "opsLegSequenceNumber",
            "psgrStatus", "fltLclOrigDt", "isWideBody", "equipmentDescription", "isCurrent","isOriginal", "isActive","flightType","pier", "arrivalBagClaimUnitedID",
            "sectorCode" ,"originOccuranceNumber", "destinationOccuranceNumber", "isSTARFlight","isPriorityFlight", "loadStatus", "localScanTimestamp", "legSequenceNumber",
            "bagTagNumber","type","isPriority","primaryBagType", "currentSectorNumber","originalSectorNumber","isAnimalOK","isCrateOK"   ]

        for col in bagRespInfo_flt_info_columns:
            final_data[f"bagRespInfo_{col}"] = flight_info.get(col)

        lastAction1 = bag_tag.get("lastAction", {})
        flt_info_lastAction1_columns = ["remark","legSequenceNumber", "storageID","stageID","flightNumber","carrierCode",
                                        "scanStation","localScanTimestamp","legSequenceNumber", "bagTagNumber","contentCode","type","isPriority","primaryBagType"]

        for col in flt_info_lastAction1_columns:
            final_data[f"lastAction1_{col}"] = lastAction1.get(col)
        claimInfo = bag_resp.get("bagRespInfo",{}).get("claimInfo",{})
        claimInfo_columns =["claimServiceResponseCode", "claimServiceResponseMessage", "referenceNumber", "worldTracerFileNumber"]
        for col in claimInfo_columns:
            final_data[f"claimInfo_{col}"] = claimInfo.get(col)

        lastAction2 = lastAction1.get("lastAction",{})
        flt_info_lastAction2_columns = ["remark","legSequenceNumber", "storageID","stageID","flightNumber","carrierCode","scanStation"]
        for col in flt_info_lastAction2_columns:
            final_data[f"lastAction2_{col}"] = lastAction2.get(col)

        bag_tag_columns = ["loadStatus", "localScanTimestamp", "legSequenceNumber", "bagTagNumber","type","isPriority","primaryBagType",
                            "currentSectorNumber","originalSectorNumber","isAnimalOK","isCrateOK"   ]


        for col in bag_tag_columns:
            final_data[col] = bag_tag.get(col)

        bag_tags = bag_resp_info.get("bagTags", {})
        bag_tags_bagtag_columns = ["localScanTimestamp", "legSequenceNumber", "bagTagNumber","type","isPriority","primaryBagType"]
        for col in bag_tags_bagtag_columns:
            final_data[f"bagTags_{col}"] = bag_tags.get(col)

        bag_tags_last_action_columns = ["remark","legSequenceNumber", "storageID","stageID","flightNumber","carrierCode","scanStation"]
        for col in bag_tags_last_action_columns:
            final_data[f"bagTags_lastAction_{col}"] = bag_tags.get("lastAction", {}).get(col)



        curr_flt_info_columns = ["flightNumber", "carrierCode", "departureStation", "arrivalStation", "localEstimateArrivalDate",
                                    "localEstimatedDepartureDate", "localScheduledArrivalDate", "localScheduledDepartureDate",
                                    "departureGate", "isInternational", "flightStatus", "arrivalBagClaimUnitedID", "departureBagSectorNumber"]

        current_inin = bag_resp_info.get('currentItin',{}).get("flightInfo", [{}])

        for column in curr_flt_info_columns:
            for index in range(5):
                final_data[f"resp_curritnr_{index}_{column}"] = (
                    current_inin[index].get(column) if len(current_inin) > index else None
                )

        originalItin_flightInfo = bag_resp_info.get("originalItin",{}).get("flightInfo",[{}])
        orig_flt_info_columns = ["flightNumber", "carrierCode", "departureStation", "arrivalStation", "localEstimateArrivalDate",
                                    "localEstimatedDepartureDate", "localScheduledArrivalDate", "localScheduledDepartureDate",
                                    "departureGate", "isInternational", "flightStatus", "arrivalBagClaimUnitedID", "sectorCode"]

        for column in orig_flt_info_columns:
                for index in range(5):
                    final_data[f"resp_origitnr_{index}_{column}"] = (
                        originalItin_flightInfo[index].get(column) if len(originalItin_flightInfo) > index else None
                    )


        bag_resp_info_columns = ["printReturnCode", "printReturnDescription", "finalDestinationStationCd", "oaConctIndicator"]
        for col in bag_resp_info_columns:
            final_data[col] = bag_resp_info.get(col)

        selectedFlights = bag_resp_info.get("selectedFlights",{}).get("flightInfo",[{}])
        selectedFlights_columns = ["flightNumber", "carrierCode", "departureStation", "arrivalStation", "localEstimateArrivalDate",
                                    "departureDate", "departureGate", "opsLegSequenceNumber"]
        for col in selectedFlights_columns:
            for index in range(5):
                final_data[f"selectedFlights_{index}_{col}"] = (
                    selectedFlights[index].get(col) if len(selectedFlights) > index else None
                )
        print_col = bag_resp_info.get("print",{})
        print_columns = ["printType", "printAddress"]
        for col in print_columns:
            final_data[f"print_{col}"] = print_col.get(col)

        bagCount = bag_resp_info.get("bagCount",{})
        bagCount_columns = ["heavyBags", "totalBagCount", "totalScanCount", "standBags", "missingBags", "quickPaks", "valetBags"]
        for col in bagCount_columns:
            final_data[f"bagCount_{col}"] = bagCount.get(col)

        missingBagSummaryBySector = bag_resp_info.get("missingBagSummaryBySector",{})
        missingBagSummaryBySector_columns = ["totalTransferBags", "totalCityBags", "totalOtherBags"]
        for col in missingBagSummaryBySector_columns:
            final_data[col] = missingBagSummaryBySector.get(col)

        missingBagSummaryBySector_tbBysector = missingBagSummaryBySector.get("totalTransferBags",{}).get("tbBysector",{})
        missingBagSummaryBySector_tbBysector_columns = ["count", "sector"]
        for col in missingBagSummaryBySector_tbBysector_columns:
            final_data[f"missingBagSummaryBySector_tbBysector_{col}"] = missingBagSummaryBySector_tbBysector.get(col)

        brTransRes = bag_resp_info.get("brTransRes",{})
        brTransRes_pax = brTransRes.get("pax",{})
        brTransRes_pax_columns = ["firstName", "lastName"]
        for col in brTransRes_pax_columns:
            final_data[f"brTransRes_pax_{col}"] = brTransRes_pax.get(col)

        brTransRes_bagtag = brTransRes.get("bagTag",{})
        brTransRes_bagtag_columns = ["loadStatus", "localScanTimestamp", "bagTagNumber", "contentCode", "isPriority", "primaryBagType", "secondaryBagType", "currentSectorNumber", "originalSectorNumber"]
        for col in brTransRes_bagtag_columns:
            final_data[f"brTransRes_bagtag_{col}"] = brTransRes_bagtag.get(col)

        brTransRes_scannedFltInfo = brTransRes.get("scannedFltInfo",{}).get("flightInfo", {})

        brTransRes_scannedFltInfo_columns = ["flightNumber", "carrierCode", "departureStation", "arrivalStation", "localEstimateArrivalDate", "localEstimatedDepartureDate", "localScheduledArrivalDate",
                                                "localScheduledDepartureDate", "departureGate", "isInternational", "acftNumber", "tailNumber", "equipmentCode","flightStatus","carrierType","isWideBody","equipmentDescription",
                                                "fltLclOrigDt","departureBagSectorNumber","isRDLFlight","tailTipIndicator","freezeWeightIndicator","isCurrent","isActive","psgrStatus","authorityToLoad","csrAuthorityToLoad"]

        for col in brTransRes_scannedFltInfo_columns:
            final_data[f"brTransRes_scannedFltInfo_{col}"] = brTransRes_scannedFltInfo.get(col)


        brTransRes_currentItin = brTransRes.get("currentItin", {}).get("flightInfo", {})
        brTransRes_currentItin_columns = ["flightNumber", "carrierCode", "departureStation", "arrivalStation", "localEstimateArrivalDate", "localEstimatedDepartureDate", "localScheduledArrivalDate", "localScheduledDepartureDate",
                                            "departureGate", "isInternational", "acftNumber", "tailNumber", "equipmentCode", "flightStatus", "carrierType", "isWideBody", "equipmentDescription",
                                            "fltLclOrigDt", "departureBagSectorNumber", "isRDLFlight", "tailTipIndicator", "freezeWeightIndicator", "isCurrent", "isActive", "psgrStatus", "isOriginal", "opsLegSequenceNumber"]
        for col in brTransRes_currentItin_columns:
            final_data[f"brTransRes_currentItin_{col}"] = brTransRes_currentItin.get(col)

        ConnectionBags = bag_resp_info.get("connectingPaxBagRec", {}).get("ConnectionBags",{})
        ConnectionBags_columns = ["bagCount", "sector", "destination", "flightNumber", "minsToDep", "departureGate"]

        for col in ConnectionBags_columns:
            final_data[f"ConnectionBags_{col}"] = ConnectionBags.get(col)

        bagTagsWCHR = bag_resp_info.get("storageCounts",{}).get("bagTagsWCHR", {})
        bagTagsWCHR_columns = ["fwdCount", "rearCount"]
        for col in bagTagsWCHR_columns:
            final_data[f"bagTagsWCHR_{col}"] = bagTagsWCHR.get(col)

        bagTagsOTHR = bag_resp_info.get("storageCounts",{}).get("bagTagsOTHR", {})
        bagTagsOTHR_columns = ["fwdCount", "rearCount"]
        for col in bagTagsOTHR_columns:
            final_data[f"bagTagsOTHR_{col}"] = bagTagsOTHR.get(col)

        scanService = bag_resp_info.get("scanService",{})
        scanService_flightInfo_columns = ["flightNumber", "carrierCode", "departureStation","arrivalStation", "departureDate"]
        for col in scanService_flightInfo_columns:
            final_data[f"scanService_flightInfo_{col}"] = scanService.get("flightInfo",{}).get(col)

        scanService_flightIndicators_columns = ["isRDLFlight", "tailTipIndicator", "freezeWeightIndicator", "epnfStatus", "acceptLoadPlanProcess", "isMELAck"]
        for col in scanService_flightIndicators_columns:
            final_data[f"scanService_flightIndicators_{col}"] = scanService.get("flightIndicators",{}).get(col)

        xclrIndicators_columns = ["arcftXCLRCompliant", "btwPaperAndXCLRIndicator", "btwPaperIndicator", "btwState", "btwXCLRIndicator", "irropIndicator", "overallPaperAndXCLRIndicator", "overallXCLRComplaint",
                                    "pltManualOverrideIndicator", "previousXCLRStatus", "xCLRStatusAtIRROPStr", "xCLRStatus"]
        for col in xclrIndicators_columns:
            final_data[f"xclrIndicators_{col}"] = scanService.get("flightIndicators",{}).get("xclrIndicators",{}).get(col)

        bagInfoList = bag_resp_info.get("bagInfoList",{})

        bagInfoList_pax_columns = ["firstName", "lastName", "loadStatus", "legSequenceNumber", "bagTagNumber", "isPriority", "primaryBagType"]

        for col in bagInfoList_pax_columns:
            final_data[f"bagInfoList_pax_{col}"] = bagInfoList.get("pax",{}).get(col)

        ppbmHeldOff_columns = ["reasonCode", "reasonDescription", "channelCode", "agentID"]

        for col in ppbmHeldOff_columns:
            final_data[f"ppbmHeldOff_{col}"] = bagInfoList.get("ppbmHeldOff",{}).get(col)
        bagInfoList_bagtag_columns = ["loadStatus","localScanTimestamp","bagTagNumber"]
        for col in bagInfoList_bagtag_columns:
            final_data[f"bagInfoList_bagtag_{col}"] = bagInfoList.get("bagTag",{}).get(col)
        return final_data
    except Exception as e:
        return None

def bm_extract(data, pk, sk, upsert_timestamp):
    try:
        json_data = json.loads(data)
        final_json_data = bm_response(json_data)
        add_on = {
            "pk": pk,
            "sk": sk,
            "upsertTimestamp": upsert_timestamp
        }
        final_json_data.update(add_on) 
        return final_json_data
    except Exception as e:
        return None
    
def scan_extract(data, pk, sk, upsert_timestamp):
    try:
        json_data = json.loads(data)
        final_json_data = scan_response(json_data)
        add_on = {
            "pk": pk,
            "sk": sk,
            "upsertTimestamp": upsert_timestamp
        }
        final_json_data.update(add_on) 
        return final_json_data
    except Exception as e:
        return None

try:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    sc._conf.set("spark.driver.maxResultSize", "16g")
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
        database="bbs-comparison",
        table_name="dynamo_data",
        transformation_ctx="DataCatalogtable_node1",
    )
    applymapping2 = ApplyMapping.apply(mappings=[
        ("sk","string", "sk","string"),
        ("pk","string", "pk","string"),
        ("scanResponse","string", "scanResponse","string"),
        ("bmResponse","string", "bmResponse","string"),
        ("upsertTimestamp","string", "upsertTimestamp","string")],
        frame = DataCatalogtable_node1,  transformation_ctx = "applymapping2")

    scan_bm_df = applymapping2.toDF()

    # Scan Response
    scan_keys = list(scan_response({}).keys())
    scan_keys = scan_keys + ["pk", "sk", "upsertTimestamp","error"]
    scan_udf = udf(scan_extract, MapType(StringType(), StringType()))
    scan_df = scan_bm_df[["scanresponse", "pk","sk", "upsertTimestamp"]]
    scan_df = scan_df.filter(scan_df.scanresponse.isNotNull())
    scan_df = scan_df.withColumn("parsed_response",scan_udf(scan_df["scanresponse"], scan_df['pk'], scan_df['sk'], scan_df['upsertTimestamp']))
    print("after extraction", scan_df.select("parsed_response").show())
    scan_df = scan_df.drop("scanresponse")
    scan_df = scan_df.filter(scan_df.parsed_response.isNotNull())
    print("after filter", scan_df.show())
    print("Final ROws", scan_df.count())
    scan_df = scan_df.select(*[col("parsed_response").getItem(key).alias(key) for key in scan_keys])
    scan_df = scan_df.drop("parsed_response")
    scan_df.write.parquet("s3a://adh-bag-comparison-scripts-us-east-1-039628302096/scanResponse.parquet",mode="overwrite")

    # # bm Response
    bm_keys = list(bm_response({}).keys())
    bm_keys = bm_keys + ["pk", "sk", "upsertTimestamp"]
    bm_udf = udf(bm_extract, MapType(StringType(), StringType()))
    bm_df = scan_bm_df[["bmresponse", "pk","sk", "upsertTimestamp"]]
    bm_df = bm_df.filter(bm_df.bmresponse.isNotNull())
    bm_df = bm_df.withColumn("parsed_response",bm_udf(bm_df["bmresponse"], bm_df['pk'], bm_df['sk'], bm_df['upsertTimestamp']))
    bm_df = bm_df.drop("bmresponse")
    bm_df = bm_df.filter(bm_df.parsed_response.isNotNull())
    bm_df = bm_df.select(*[col("parsed_response").getItem(key).alias(key) for key in bm_keys])
    bm_df = bm_df.drop("parsed_response")
    bm_df.write.parquet("s3a://adh-bag-comparison-scripts-us-east-1-039628302096/bmResponse.parquet",mode="overwrite")

except Exception as e:
    print(e)
job.commit()
