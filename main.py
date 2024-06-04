import time

import requests
from requests.adapters import HTTPAdapter, Retry

from config import BLAZE_AUTH, BLAZE_URL
from sample_collection import SampleCollection


COLLECTIONS_TO_ADD = [
    {
        "identifier": "bbmri-eric:ID:CZ_MMCI:collection:Blood_samples",
        "name": "Blood samples",
        "acronym": "BS"
    },
    {
        "identifier": "bbmri-eric:ID:CZ_MMCI:collection:Cells",
        "name": "Cells",
        "acronym": ""
    },
    {
        "identifier": "bbmri-eric:ID:CZ_MMCI:collection:DNA",
        "name": "DNA",
        "acronym": "DNA"
    },
    {
        "identifier": "bbmri-eric:ID:CZ_MMCI:collection:Other",
        "name": "Other",
        "acronym": ""
    },
    {
        "identifier": "bbmri-eric:ID:CZ_MMCI:collection:Tissue",
        "name": "Tissue",
        "acronym": ""
    }
]

TYPE_TO_COLLECTION = {
    "tissue-frozen": "bbmri-eric:ID:CZ_MMCI:collection:Cells",
    "tissue-other": "bbmri-eric:ID:CZ_MMCI:collection:Cells",
    "peripheral-blood-cells-vital": "bbmri-eric:ID:CZ_MMCI:collection:Cells",
    "blood-plasma": "bbmri-eric:ID:CZ_MMCI:collection:Blood_samples",
    "liquid-other": "bbmri-eric:ID:CZ_MMCI:collection:Blood_samples",
    "serum": "bbmri-eric:ID:CZ_MMCI:collection:Blood_samples",
    "dna": "bbmri-eric:ID:CZ_MMCI:collection:DNA",
}


ORGANIZATION_TO_ID: dict = {

}


retries = Retry(total=10, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
session = requests.session()
session.mount('http://', HTTPAdapter(max_retries=retries))
session.auth = BLAZE_AUTH


def is_endpoint_available(endpoint_url, max_attempts=10, wait_time=60) -> bool:
    """
    Check for the availability of an http endpoint
    :param endpoint_url: URL for the endpoint
    :param max_attempts: max number of attempts for connection retries
    :param wait_time: seconds in between unsuccessful connection attempts
    :return: true if reachable, false otherwise
    """
    attempts = 0
    print(f"Attempting to reach endpoint: '{endpoint_url}'.")
    while attempts < max_attempts:
        try:
            response = session.get(endpoint_url, verify=False, auth=BLAZE_AUTH)
            response.raise_for_status()
            print(f"Endpoint '{endpoint_url}' is available.")
            return True
        except requests.exceptions.RequestException as e:
            print(
                f"Attempt {attempts + 1}/{max_attempts}: Endpoint not available yet. Retrying in {wait_time} seconds.")
            attempts += 1
            time.sleep(wait_time)

    print(f"Endpoint '{endpoint_url}' was not available after {max_attempts} attempts.")
    return False


def is_resource_present_in_blaze(resource_type: str, identifier: str) -> bool:
    """
    Checks if a resource of specific type with a specific identifier is present in the Blaze store.
    :param resource_type: FHIR resource type.
    :param identifier: Identifier belonging to the resource.
    It is not the FHIR resource ID!
    :return:
    """
    try:
        count = (session.get(
            url=BLAZE_URL + f"/{resource_type.capitalize()}?identifier={identifier}&_summary=count",
            verify=False)
                 .json()
                 .get("total"))
        return count > 0
    except TypeError:
        return False


def populate_collections():
    try:
        for collection in COLLECTIONS_TO_ADD:
            fhir_collection = SampleCollection(identifier=collection["identifier"])
            if not is_resource_present_in_blaze("Organization", fhir_collection.identifier):
                session.post(url=BLAZE_URL + "/Organization", json=fhir_collection.to_fhir(), verify=False)
    except requests.exceptions.ConnectionError:
        print("Cannot connect to blaze!")
        return 0


def populate_collections_ids():
    try:
        request_response = session.get(url=BLAZE_URL + "/Organization")
        response_json = request_response.json()
        for entry in response_json["entry"]:
            resource = entry["resource"]
            ORGANIZATION_TO_ID[resource["identifier"][0]["value"]] = resource["id"]
    except requests.exceptions.ConnectionError:
        print("Cannot connect to blaze!")
        return 0


def update_resources(resource_type: str):
    try:
        request_response = session.get(url=BLAZE_URL + f"/{resource_type.capitalize()}",
                                       verify=False)
        while request_response.status_code == 200:
            response_json = request_response.json()
            for entry in response_json["entry"]:
                resource = entry["resource"]
                print("working on resource with id", resource["id"])
                updated = False
                collection_present = False
                for extension in resource["extension"]:
                    if extension["url"] == 'https://fhir.bbmri.de/StructureDefinition/Custodian':
                        collection_present = True

                        # collection_id = TYPE_TO_COLLECTION[
                            # resource["extension"][0]["valueCodeableConcept"]["coding"][0]["code"]]
                        # MMCI
                        collection_id = TYPE_TO_COLLECTION[resource["type"]["coding"][0]["code"]]
                        if extension["valueReference"]["reference"] != "Organization/" + ORGANIZATION_TO_ID[
                            collection_id]:
                            extension["valueReference"]["reference"] = "Organization/" + ORGANIZATION_TO_ID[
                                collection_id]
                            updated = True
                            print(f"resource with id {resource['id']} Updated")
                if not collection_present:
                    updated = True
                    # collection_id = TYPE_TO_COLLECTION[
                    #     resource["extension"][0]["valueCodeableConcept"]["coding"][0]["code"]]
                    # MMCI
                    collection_id = TYPE_TO_COLLECTION[resource["type"]["coding"][0]["code"]]
                    extension = {"url": "https://fhir.bbmri.de/StructureDefinition/Custodian",
                                 "valueReference": {"reference": "Organization/" + ORGANIZATION_TO_ID[collection_id]}}
                    resource["extension"].append(extension)
                    print(f"resource with id {resource['id']} got a new extension")
                if updated:
                    request_response = session.put(url=BLAZE_URL + f"/{resource_type.capitalize()}/{resource['id']}",
                                                   json=resource, verify=False)
                    print(f"updating resource with id {resource['id']} resulted in {request_response.status_code}")
            next_link_dict = response_json["link"][-1]
            if next_link_dict["relation"] != "next":
                break
            url_fhir_part_index = next_link_dict["url"].find("/fhir")
            if url_fhir_part_index == -1:
                break
            next_link = BLAZE_URL + next_link_dict["url"][url_fhir_part_index + len("/fhir"):]
            request_response = session.get(url=next_link)

    except requests.exceptions.ConnectionError:
        print("Cannot connect to blaze!")
        return 0


if __name__ == '__main__':
    is_endpoint_available(BLAZE_URL,5,5)
    populate_collections()
    populate_collections_ids()
    update_resources("Specimen")
