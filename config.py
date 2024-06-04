import os

BLAZE_URL = os.getenv("BLAZE_URL", "http://127.0.0.1:8080/fhir")
BLAZE_AUTH: tuple = (os.getenv("BLAZE_USER", ""), os.getenv("BLAZE_PASS", ""))