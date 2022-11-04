import io
import pandas as pd
from analytics.utils.time_util import current_epoch_milliseconds

from elasticsearch import Elasticsearch
es = Elasticsearch()

ts_start = str(current_epoch_milliseconds() - 86400000) #24 hours to milliseconds

query = """
{
   "query": {
    "bool": {
      "must": [
        {
          "terms": {
            "org_id": [
              "3831255c-e5f3-4b3f-9ab5-6cc640530e40",
              "61e61d9d-5395-4b68-86c8-9cfc38f10fa4",
              "a8c903ae-e6d0-42a7-b815-fe395dea8017",
              "c1cac1c4-1753-4dde-a065-e17a1c305c2d",
              "1a40a6fb-4d5b-422e-b0b2-c5eda2cb8e2a",
              "f5451dc6-aa80-4d1c-a49a-dede30b6d878",
              "56de201d-e63b-4312-9858-40f4cfe35c7f",
              "5a578f3c-f094-4d13-a7dd-96f0aaecb5b6",
              "d8cec22e-e0c2-11e5-8d0f-02e208b2d34f"
            ]
          }
        },
        {
          "match": {
            "event_name": "frequent_reinit"
          }
        },
        {
          "terms": {
            "ap.model": [
              "AP41-US",
              "AP41E-US"
            ]
          }
        },
        {
          "range": {
            "severity": {
              "gte": 60
            }
          }
        },
        {
          "range": {
            "start_time": {
              "gte": """ + ts_start + """
            }
          }
        }
      ]
    }
  },
"aggs": {
    "NAME": {
      "terms": {
        "field": "org_id",
        "size": 10
      },
      "aggs": {
        "NAME": {
          "terms": {
            "field": "ap_id",
            "size": 3
          },
          "aggs":{
            "NAME": {
              "terms": {
                "field": "radio.dev.keyword",
                "size": 3
              }
            }
          }
        }
      }
    }
  }
}
"""

gcp_query = """
{
   "query": {
    "bool": {
      "must": [
        {
          "terms": {
            "org_id": [
              "6d49ba5d-26f5-4d32-a787-abdaeb31a994",
              "dfdd2428-7627-49ed-9800-87f7c61972d3",
              "16c9bbfe-3a4d-479d-bef8-4b5d59c44369",
              "80f370d0-f52b-4ae0-bfe5-7db30863f7d8"
            ]
          }
        },
        {
          "match": {
            "event_name": "frequent_reinit"
          }
        },
        {
          "terms": {
            "ap.model": [
              "AP41-US",
              "AP41E-US"
            ]
          }
        },
        {
          "range": {
            "severity": {
              "gte": 60
            }
          }
        },
        {
          "range": {
            "start_time": {
              "gte": """ + ts_start + """
            }
          }
        }
      ]
    }
  },
"aggs": {
    "NAME": {
      "terms": {
        "field": "org_id",
        "size": 10
      },
      "aggs": {
        "NAME": {
          "terms": {
            "field": "ap_id",
            "size": 3
          },
          "aggs":{
            "NAME": {
              "terms": {
                "field": "radio.dev.keyword",
                "size": 3
              }
            }
          }
        }
      }
    }
  }
}
"""
def format_header(sheet, writer, df):
    workbook = writer.book
    sheet.freeze_panes(1, 0)
    header_format = workbook.add_format(
        {"bold": True, "text_wrap": True, "valign": "top", "bg_color": "#D7E4BC", "border": 1}
    )
    for col_num, value in enumerate(df.columns):
        sheet.write(0, col_num, value, header_format)


PROD_ES_SERVER = 'http://es715-access-000-production.mist.pvt'
PROD_ES_INDEX = 'entity_event_2022*'

headers = {
    'Content-Type': 'application/json'
}

es = Elasticsearch([PROD_ES_SERVER])
res = es.search(index=PROD_ES_INDEX, body=gcp_query)

if len(res['hits']['hits']) == 0:
    df_es2 = pd.DataFrame()

elif res.get("aggregations") is not None and res['aggregations'].get('NAME') is not None and res['aggregations']['NAME'].get("buckets") is not None:
        buckets = res['aggregations']['NAME']['buckets']
        pd.set_option('display.max_columns', None)
        df_list = []

        for bucket in buckets:
            org_id = bucket.get("key")
            org_data = {"org_id": org_id}
            sub_buckets = bucket['NAME']['buckets']
            org_data_list = []
            #i = 0
            for sub_bucket in sub_buckets:
                ap_id = sub_bucket['key']
                #i = i+1
                ap_dict = {}
                data_buckets = sub_bucket['NAME']['buckets']
                for data in data_buckets:
                    radio = data['key']
                    count = data['doc_count']
                    #ap_data = {}
                    ap_data = {"ap": ap_id, "radio": radio, "count": count}
                    # ap_data[radio] = count
                    # ap_dict[ap_id] =  ap_data
                    org_data_list.append(ap_data)

            df = pd.DataFrame(org_data, index=[0])
            df2 = pd.DataFrame(org_data_list)
            df_list.append(df)
            df_list.append(df2)
            # print(df)
            # print(df2)


        with io.BytesIO() as output:
            with pd.ExcelWriter(output, engine="xlsxwriter", options={"strings_to_numbers": True}) as writer:
                row = 0
                for df in df_list:
                    if df is not None:
                        extracted_data = df[df.columns]
                        extracted_data.to_excel(writer, sheet_name="Frequent reinit APs", index=False, startrow=row, startcol=0)
                        sheet = writer.sheets["Frequent reinit APs"]
                        #format_header(sheet, writer, extracted_data)
                        row = row + len(df.index) + 1
                        if "ap" in df.columns:
                            row = row + 2

            output.seek(0)  # Seek BytesIO object to position 0 before uploading to cloud

            # print(output)
            temporarylocation = "/Users/anagarkar/Downloads/frequent_reinit_3rdNov_GCP.xlsx"
            with open(temporarylocation, 'wb+') as out:  ## Open temporary file as bytes
                out.write(output.read())

        print("data written successfully")
