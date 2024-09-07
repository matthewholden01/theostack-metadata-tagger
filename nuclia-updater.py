import os, pickle, re
from nuclia import sdk

def loadData(filename):
  #filename = str(filepath).split('/')[-1]
  #filename = re.sub(r'\.pdf', '', filename)
  with open(filename, 'rb') as file:
    data = pickle.load(file)
  return data

NUCLIA_API_KEY = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6InNhIn0.eyJpc3MiOiJodHRwczovL2F3cy11cy1lYXN0LTItMS5udWNsaWEuY2xvdWQvIiwiaWF0IjoxNzI1Njk3MTY2LCJzdWIiOiJiZWNmMjEwNC0zMzIxLTQ0MTMtYjc1My1kNGQwZTE4ZmQxMWEiLCJqdGkiOiJlZGY3OWI0Ny0yYzEzLTRlZDYtODg1OC04ZWEzNWI4MjQwZGEiLCJleHAiOjE3NTcyMzMxNjYsImtleSI6IjhhOGNmODQ2LWE3N2ItNDhmMC1iZjhiLWFkNmRhYjQ2OWZiOCIsImtpZCI6ImUwOTM5MmE0LTI0OTgtNDMxYS1iY2QxLTQ1NDZjYjQ3ZjBjMiJ9.ca_d7mjbbvbyDcTKgNiXQ748d0DovtU4ORJBG1bl_M5iOQC3gabd3Y9dsl8oxE9o5KQ0h02X-cS7ZY26cSqR5TkQ6EBDa3R0K7Wzv89klZ4Hp9V-UD1-qiDDEiiHstiaecTowW2ZmvlTp3JgS2nl9xhXINuMZSpbZL5O8nPcJFo3QRvMU-efiX4ShUmQhHlgS9JUJvjaLJTpA3sp7trqPSJAtkcINYjZv6aMr5WKWaNctzQXDKLUeNyMhg9EbMfYei1x5nyTQnCQN0Wp163KURCBeoZXRlHQghXSgoVAtS4qIHwGMZVs3l4KN9mwFo8rgJS_c9JUwdWTwOuJ3mw303Y94gGLjD4cPc879o4dSVSDAleaCNuV85E_9JHRQrsYZ5aqE8jXTnSql6cg8uHJTRVrkRafnAAlZmE40219Rdn3WowlSCrtSABBGstMW4ECJQNqIW6NJ8nHrE-b4LMUXmtM7pZF7E0ZdJcden-s8yV_rEwI_DRodjqmsSRZnvmvqyjRKid131Fo38tGLSddoh7bmquxm4FBxMlPNKZi_wIkp_9LDhADuv5balRG_UIpY7L4lBb6CJoBDb33qckok26EcgKKIGZUk0QAfxfv9H57e3SfDwEEE3ZRZuxtxgoGKicewqZvguSSY_heN30RmYrFMGDkuabud99NI4M7KPo"
KNOWLEDGE_BOX_URL = "https://aws-us-east-2-1.nuclia.cloud/api/v1/kb/c3043a5c-8c96-48d9-b236-d4f74d092880"

sdk.NucliaAuth().kb(url=KNOWLEDGE_BOX_URL, token=NUCLIA_API_KEY)


dir_path = '/Users/matthewholden/Documents/TheoStack/Docs/'
enum_names = ["Tradition", "Theology", "Doctrine", "Resource"]
for file in os.listdir(dir_path):
    filename = os.fsdecode(file)

    processed_filename = re.sub(r"\.pdf", "_metadata", filename)

    enhanced_documents = loadData(processed_filename)

    metadata_counter = {enum: {} for enum in enum_names}

    for doc in enhanced_documents:
      for key, value in doc.metadata.items():
        if key in enum_names:
          tag = value.split(":")[0]
          try:
            metadata_counter[key][tag]["count"] += 1
            metadata_counter[key][tag]["word_count"] += doc.metadata["word_count"]
          except KeyError: 
            metadata_counter[key][tag] = { "count": 1, "word_count": doc.metadata["word_count"] }

    max_labels = {}
    for key, value in metadata_counter.items():
      max_meta = ""
      max_count = 0
      max_words = 0
      for label, values in value.items():
        if values["count"] > max_count:
          max_meta = label
          max_count = values["count"]
          max_words = values["word_count"]
        elif values["count"] == max_count:
          if values["word_count"] > max_words:
            max_words = values["word_count"]
            max_meta = label
      max_labels[key] = max_meta
    print(max_labels)

    

    sdk.NucliaResource().update(
      rid="4013c7e7a161405aef1e35f71adc57eb",
      usermetadata={"classifications": [{"labelset": key, "label": value} for key, value in max_labels.items()]}
    )

