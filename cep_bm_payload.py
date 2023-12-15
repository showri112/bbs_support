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
import xml.etree.ElementTree as ET

def xml_to_dict(element):
    if len(element) == 0:
        return element.text
    result = {}
    for child in element:
        child_dict = xml_to_dict(child)
        if child.tag in result:
            if isinstance(result[child.tag], list):
                result[child.tag].append(child_dict)
            else:
                result[child.tag] = [result[child.tag], child_dict]
        else:
            result[child.tag] = child_dict

    for key, value in element.attrib.items():
        result[key] = value

    return result

def remove_namespace(element):
    if '}' in element.tag:
        element.tag = element.tag.split('}', 1)[1]
    for child in element:
        remove_namespace(child)
def cep_payload(json_data, pk, sk, upsert_timestamp):
    try:
        data = json.loads(json_data)
        xml_text = data.get("text", "")
        root = ET.fromstring(xml_text)
        remove_namespace(root)
        xml_dict = {root.tag: xml_to_dict(root)}
        final_data = cep_payload_extract(xml_dict)
        addon_dict = {"pk": pk, "sk": sk, "upsertTimestamp": upsert_timestamp}
        final_data.update(addon_dict)
        return final_data
    except Exception as e:
        return None
def bm_payload(json_data, pk, sk, upsert_timestamp):
    try:
        data = json.loads(json_data)
        xml_text = data.get("text", "")
        root = ET.fromstring(xml_text)
        remove_namespace(root)
        xml_dict = {root.tag: xml_to_dict(root)}
        final_data = bm_payload_extract(xml_dict)
        addon_dict = {"pk": pk, "sk": sk, "upsertTimestamp": upsert_timestamp}
        final_data.update(addon_dict)
        return final_data
    except Exception as e:
        return None
def cep_payload_extract(json_data):
    try:
        final_data = {}
        body = json_data.get("Envelope",{}).get("Body",{})
        event = json_data.get("Envelope",{}).get("eventHeader",{})
        # Event data
        event_keys = ["eventName", "eventCreationSys", "eventCreationDtm", "eventActionCd", "eventID", "version"]
        for key in event_keys:
            final_data[key] = event.get(key)

        # Body data
        bag_details = body.get("bagDetails", {})
        bag_info = bag_details.get("bagInfo", {})
        bagScanDtl = bag_details.get("bagScans", {}).get("bagScanDtl",{})

        final_data["tagNbr"] = bag_details.get('tagNbr')
        final_data["tagUniqKey"] = bag_details.get('tagUniqKey')
        final_data["tagIssDtm"] = bag_details.get('tagIssDtm')
        final_data["bagTagActvInd"] = bag_details.get('bagTagActvInd')

        # Extract bagInfo data
        bag_info_keys = ["bagOrigArpt", "bagTermArpt", "tagPrimaryType", "primaryTypePriority", "isPriority", "isHeavy", "isRush", "isSelectee", "printerId"]
        for key in bag_info_keys:
            final_data[key] = bag_info.get(key)

        # Extract paxInfo data
        pax_info = bag_details.get("paxInfo", {})
        pax_info_keys = ["psgrLastName", "psgrFirstName", "psgrPnr"]
        for key in pax_info_keys:
            final_data[key] = pax_info.get(key)

        # Extract bagScanDtl data
        if bagScanDtl:
            scan_keys = ["carrIataCd", "fltNbr", "origArptCd", "destArptCd", "scanLclDtm", "uploadScanLclDtm", "scanLocation", "pbRemark", "scanType", "agntId"]
            for key in scan_keys:
                final_data[key] = bagScanDtl.get(key)

        # Extract current and original itineraries
        current_itne = bag_details.get("bagCurrentItineraries", {}).get("bagItineraryDtl", [{}])
        original_itne = bag_details.get("bagOriginalItineraries", {}).get("bagItineraryDtl", [{}])
        curr_itne_keys = [
            "fltLegActvInd","carrIataCd","fltNbr","legLclDepDt","legGmtDepDt","fltLclOrigDt","origArptCd","destArptCd",
            "cogInd","psgrChkInSeqNbr","legSqnrNbr","bagClassCd","bagSecurityStatCd","csrAuthReasonCd",
            "csrAuthReasonDesc","atlReasonCd","atlReasonDesc","authToLoad","bagStatus","origSector","currSector",
            "psgrStatus","handlingStatus","reflightType","storageId","isAdvanced","isBM" ]
        original_itne_keys =  [
            "fltLegActvInd","carrIataCd","fltNbr","legLclDepDt","legGmtDepDt","fltLclOrigDt","origArptCd","destArptCd",
            "cogInd","psgrChkInSeqNbr","legSqnrNbr","bagClassCd","bagSecurityStatCd","csrAuthReasonCd","csrAuthReasonDesc",
            "atlReasonCd","atlReasonDesc","authToLoad","origSector","currSector","psgrStatus","storageId","isAdvanced" ]
        for index in range(1):
            for column in curr_itne_keys:
                final_data[f"curr_itne_{index}_{column}"] = current_itne[index].get(column) if index < len(current_itne) else None

            for column in original_itne_keys:
                final_data[f"original_itne_{index}_{column}"] = original_itne[index].get(column) if index < len(original_itne) else None

        return final_data
    except Exception as e:
        return None

def bm_payload_extract(json_data):
    try:
        body = json_data.get("Envelope",{}).get("Body",{})
        event = json_data.get("Envelope",{}).get("eventHeader",{})
        final_data = {}

        # Event data
        event_keys = ["eventName", "eventCreationSys", "eventCreationDtm", "eventActionCd", "eventID", "version"]
        for key in event_keys:
            final_data[key] = event.get(key)

        # Body data
        bag_details = body.get("bagDetails", {})
        bag_info = bag_details.get("bagInfo", {})
        bagScanDtl = bag_details.get("bagScans", {}).get("bagScanDtl")

        final_data["tagNbr"] = bag_details.get('tagNbr')
        final_data["tagUniqKey"] = bag_details.get('tagUniqKey')
        final_data["tagIssDtm"] = bag_details.get('tagIssDtm')
        final_data["bagTagActvInd"] = bag_details.get('bagTagActvInd')

        # Extract bagInfo data
        bag_info_keys = ["bagOrigArpt", "bagTermArpt", "tagPrimaryType", "primaryTypePriority", "isPriority", "isHeavy", "isRush", "isSelectee", "printerId"]
        for key in bag_info_keys:
            final_data[key] = bag_info.get(key)

        # Extract tagBagType data
        tagBagType = bag_info.get("tagBagType", {})
        final_data["TagTypeCd"] = tagBagType.get("BagTypeCd")
        final_data["Priority"] = tagBagType.get("Priority")

        # Extract paxInfo data
        pax_info = bag_details.get("paxInfo", {})
        pax_info_keys = ["psgrLastName", "psgrFirstName", "psgrPnr"]
        for key in pax_info_keys:
            final_data[key] = pax_info.get(key)

        # Extract bagScanDtl data
        if bagScanDtl:
            scan_keys = ["scanLclDtm", "uploadScanLclDtm", "scanLocation", "pbRemark", "scanType", "agntId"]
            for key in scan_keys:
                final_data[key] = bagScanDtl.get(key)

        # Extract current and original itineraries
        current_itne = bag_details.get("bagCurrentItineraries", {}).get("bagItineraryDtl", [{}])
        original_itne = bag_details.get("bagOriginalItineraries", {}).get("bagItineraryDtl", [{}])

        curr_itne_keys = ["fltLegActvInd", "carrIataCd", "fltNbr", "legLclDepDt", "legGmtDepDt", "fltLclOrigDt", "origArptCd", "destArptCd", "cogInd", "psgrChkInSeqNbr", "legSqnrNbr", "bagClassCd", "bagSecurityStatCd", "isActionLeg", "bagStatus", "psgrStatus", "isAdvanced"]
        original_itne_keys = curr_itne_keys

        for index in range(1):
            for column in curr_itne_keys:
                final_data[f"curr_itne_{index}_{column}"] = current_itne[index].get(column) if index < len(current_itne) else None

            for column in original_itne_keys:
                final_data[f"original_itne_{index}_{column}"] = original_itne[index].get(column) if index < len(original_itne) else None

        return final_data
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
        table_name="datas",
        transformation_ctx="DataCatalogtable_node1",
    )
    cep_bm_df = ApplyMapping.apply(mappings=[
        ("sk","string", "sk","string"),
        ("pk","string", "pk","string"),
        ("cepPayload","string", "cepPayload","string"),
        ("bmPayload","string", "bmPayload","string"),
        ("upsertTimestamp","string", "upsertTimestamp","string")],
        frame =DataCatalogtable_node1,  transformation_ctx = "applymapping2")

    cep_bm_df = cep_bm_df.toDF()

    # Cep Payload
    cep_udf = udf(cep_payload, MapType(StringType(), StringType()))
    cep_df = cep_bm_df[["cepPayload", "pk","sk", "upsertTimestamp"]]
    cep_df = cep_df.withColumn("parsed_response",cep_udf(cep_df["cepPayload"], cep_df['pk'], cep_df['sk'], cep_df['upsertTimestamp']))
    print("after extract", cep_df.show())
    cep_df = cep_df.drop("cepPayload")
    cep_df = cep_df.filter(cep_df.parsed_response.isNotNull())
    print("after filter", cep_df.show())
    cep_keys = list(cep_payload_extract({}).keys())
    cep_keys = cep_keys + ["pk", "sk", "upsertTimestamp"]
    cep_df = cep_df.select(*[col("parsed_response").getItem(key).alias(key) for key in cep_keys])
    print("after split", cep_df.show())
    cep_df = cep_df.drop("parsed_response")
    print("after drop", cep_df)
    cep_df = cep_df.toPandas()
    cep_df.to_csv('s3://adh-bag-comparison-scripts-us-east-1-039628302096/cepPayloadData/cepPayload.csv', index=False)

    # bm payload
    bm_udf = udf(bm_payload, MapType(StringType(), StringType()))
    bm_df = cep_bm_df[["bmPayload", "pk","sk", "upsertTimestamp"]]
    bm_df = bm_df.withColumn("parsed_response",bm_udf(bm_df["bmPayload"], bm_df['pk'], bm_df['sk'], bm_df['upsertTimestamp']))
    bm_df = bm_df.drop("bmPayload")
    bm_df = bm_df.filter(bm_df.parsed_response.isNotNull())
    bm_keys = list(bm_payload_extract({}).keys())
    bm_keys = bm_keys + ["pk", "sk", "upsertTimestamp"]
    bm_df = bm_df.select(*[col("parsed_response").getItem(key).alias(key) for key in bm_keys])
    bm_df = bm_df.drop("parsed_response")
    bm_df = bm_df.toPandas()
    bm_df.to_csv('s3://adh-bag-comparison-scripts-us-east-1-039628302096/bmPayloadData/bmPayload.csv', index=False)

except Exception as e:
    print(e)

job.commit()
