import logging
from datetime import datetime

from scoap3.articles.documents import ArticleDocument
from scoap3.articles.models import ArticleFile
from scoap3.authors.models import Author
from scoap3.misc.models import Affiliation
from scoap3.articles.util import (
    get_arxiv_primary_category,
    get_first_arxiv,
    get_first_doi,
)

logger = logging.getLogger(__name__)
import csv
import xml.etree.ElementTree as ET
import re


def parse_article_xml(article, publisher):
    files = article.related_files
    xml_files = [f for f in files if f.file.endswith(".xml")]

    parsed_authors = []
    non_unique_affiliations = []

    for file in xml_files:
        url = file.file
        file_obj = ArticleFile.objects.filter(
            file__contains=url.split("ch/media/")[-1]
        ).first()

        if file_obj:
            parsed_authors, non_unique_affiliations = parse_xml_from_s3(file_obj, publisher)

    return parsed_authors, non_unique_affiliations


def parse_xml_from_s3(file_path, publisher):
    with file_path.file.open() as file:
        xml_content = file.read()
        xml_content = xml_content.decode("utf8")

    root = ET.fromstring(xml_content)
    authors = []
    affiliations = []

    if publisher in ["APS", "Hindawi"]:
        authors, non_unique_affiliations = parse_aps_hindawi_xml(root)
        return authors, non_unique_affiliations

    elif publisher == "Springer":
        authors, non_unique_affiliations = parse_springer_xml(root)
        return authors, non_unique_affiliations

    elif publisher == "OUP":
        authors, affiliations = parse_oup_xml(root)
        return authors, affiliations

    return authors, affiliations


def parse_aps_hindawi_xml(root):
    authors_data = []
    affiliations_list = []
    affiliations = {}

    for aff_element in root.findall(".//aff"):
        aff_id = aff_element.get("id")
        institution = (
            aff_element.find("institution-wrap/institution").text
            if aff_element.find("institution-wrap/institution") is not None
            else None
        )
        ror = (
            aff_element.find(
                "institution-wrap/institution-id[@institution-id-type='ror']"
            ).text
            if aff_element.find(
                "institution-wrap/institution-id[@institution-id-type='ror']"
            )
            is not None
            else None
        )

        affiliations[aff_id] = {"name": institution, "ror": ror}
        affiliations_list.append({"id": aff_id, "name": institution, "ror": ror})

    non_unique_affiliations = []
    for author in root.findall(".//contrib-group/contrib[@contrib-type='author']"):
        author_info = {
            "given_name": "1",
            "family_name": "2",
            "orcid": author.find("./contrib-id[@contrib-id-type='orcid']").text
            if author.find("./contrib-id[@contrib-id-type='orcid']") is not None
            else None,
            "affiliations": [],
        }
        for aff_ref in author.findall("xref[@ref-type='aff']"):
            aff_id = aff_ref.get("rid")
            for single_aff in aff_id.split():
                aff_info = affiliations.get(single_aff, {})
                non_unique_affiliations.append({
                    "ref-type": "aff", 
                    "rid": single_aff,
                    "ror": aff_info.get("ror")
                })
            if aff_id in affiliations:
                author_info["affiliations"].append(affiliations[aff_id])

        authors_data.append(author_info)

    return authors_data, non_unique_affiliations


def parse_springer_xml(root):
    affiliation_map = {}
    non_unique_affiliations = []

    for affiliation in root.findall(".//Affiliation"):
        aff_id = affiliation.get("ID")
        institution_name = affiliation.findtext("OrgName")
        ror_id = affiliation.findtext("OrgID[@Type='ROR']")

        if aff_id:
            affiliation_map[aff_id] = {
                "InstitutionName": institution_name,
                "ror": ror_id,
            }

    authors = []
    
    for author in root.findall(".//AuthorGroup/Author"):
        given_name = author.findtext("AuthorName/GivenName")
        family_name = author.findtext("AuthorName/FamilyName")
        orcid = author.get("ORCID")
        affiliation_ids = author.get("AffiliationIDS", "").split()

        author_data = {
            "given_name": f"{given_name}" if given_name else "",
            "family_name": f"{family_name}" if family_name else "",
            "orcid": orcid,
            "Affiliations": [],
        }
        
        for aff_id in affiliation_ids:
            aff_info = affiliation_map.get(aff_id, {})
            author_data["Affiliations"].append(aff_info)
            non_unique_affiliations.append({
                "ref-type": "aff",
                "rid": aff_id,
                "ror": aff_info.get("ror")
            })

        authors.append(author_data)
    
    for inst_author in root.findall(".//AuthorGroup/InstitutionalAuthor"):
        for author in inst_author.findall("Author"):
            given_name = author.findtext("AuthorName/GivenName")
            family_name = author.findtext("AuthorName/FamilyName")
            orcid = author.get("ORCID")
            affiliation_ids = author.get("AffiliationIDS", "").split()

            author_data = {
                "given_name": f"{given_name}" if given_name else "",
                "family_name": f"{family_name}" if family_name else "",
                "orcid": orcid,
                "Affiliations": [],
            }
            
            if affiliation_ids:
                for aff_id in affiliation_ids:
                    aff_info = affiliation_map.get(aff_id, {})
                    author_data["Affiliations"].append(aff_info)
                    non_unique_affiliations.append({
                        "ref-type": "aff",
                        "rid": aff_id,
                        "ror": aff_info.get("ror")
                    })

            authors.append(author_data)

    return authors, non_unique_affiliations


def parse_oup_xml(root):
    authors_data = []
    
    for author in root.findall("front/article-meta/contrib-group/contrib[@contrib-type='author']"):
        given_name = author.findtext("name/given-names")
        family_name = author.findtext("name/surname")
        orcid = author.findtext("contrib-id[@contrib-id-type='orcid']")
        
        author_info = {
            "given_name": given_name if given_name else "",
            "family_name": family_name if family_name else "",
            "orcid": orcid if orcid else None,
            "affiliations": []
        }
        authors_data.append(author_info)
    
    return authors_data, []


def parse_elsevier_datasets(file):
    file_content = file.read()
    xml_content = file_content.decode("utf-8", errors="ignore")
    ror_pattern = r'<ce:data-availability.*?</ce:data-availability>'
    return re.findall(ror_pattern, xml_content)


def parse_hindawi_datasets(file):
    xml_content = file.read()
    xml_content = xml_content.decode("utf8")
    root = ET.fromstring(xml_content)

    datasets = []
    for section in root.findall(".//sec[@sec-type='data-availability']"):
        datasets.append(ET.tostring(section, encoding="unicode").strip())
    return datasets


def parse_datasets_from_xml(article, publisher):
    files = article.related_files
    xml_files = [f for f in files if f.file.endswith(".xml")]

    for file in xml_files:
        url = file.file
        file_obj = ArticleFile.objects.filter(
            file__contains=url.split("ch/media/")[-1]
        )[0]
        with file_obj.file.open() as file:
            if publisher == "Elsevier":
                return parse_elsevier_datasets(file)
            elif publisher == "Hindawi":
                return parse_hindawi_datasets(file)

    return []


def get_publisher_headers(publisher):
    base_headers = [
        "year",
        "journal",
        "doi",
        "publication date",
        "arxiv number",
        "primary arxiv category",
        "total number of authors",
        "total number of related materials, type dataset",
        "total number of related materials, type software",
    ]
    
    publisher_headers = {
        "Springer": [
            "total number of ORCIDs linked to the authors",
            "total number of non-unique affiliations",
            "total number of RORs linked with the non-unique affiliations",
        ],
        "APS": [
            "total number of ORCIDs linked to the authors",
            "total number of non-unique affiliations",
            "total number of RORs linked with the non-unique affiliations",
        ],
        "Hindawi": [
            "total number of ORCIDs linked to the authors",
            "total number of non-unique affiliations",
            "link to datasets",
        ],
        "OUP": [
            "total number of ORCIDs linked to the authors",
            "total number of non-unique affiliations",
        ],
        "Elsevier": [
            "total number of non-unique affiliations",
            "link to datasets",
        ]
    }
    
    return base_headers + publisher_headers.get(publisher, [])

def process_article_data(article, publisher):
    base_data = [
        article.publication_date.year,
        article.publication_info[0].journal_title,
        get_first_doi(article),
        article.publication_date,
        get_first_arxiv(article),
        get_arxiv_primary_category(article),
        len(article.to_dict().get("authors", [])),
    ]

    additional_data = {}
    
    if publisher in ["Springer", "APS", "Hindawi", "OUP"]:
        parsed_authors, non_unique_affiliations = parse_article_xml(article, publisher)
        additional_data["orcids"] = len([a for a in parsed_authors if a.get('orcid')])
        
        if publisher in ["Springer", "APS"]:
            rors = [aff.get('ror') for aff in non_unique_affiliations if aff.get('ror')]
            additional_data["rors"] = len(rors)
            additional_data["affiliations"] = len(non_unique_affiliations)
        elif publisher == "Hindawi":
            additional_data["affiliations"] = len(non_unique_affiliations)
            additional_data["datasets"] = parse_datasets_from_xml(article, publisher)
        elif publisher == "OUP":
            repo_affiliations = []
            author_objs = Author.objects.filter(article_id=article.id)
            for author in author_objs:
                affiliation_objs = Affiliation.objects.filter(author_id=author.id)
                repo_affiliations.extend(affiliation_objs)
            additional_data["affiliations"] = len(repo_affiliations)
    
    elif publisher == "Elsevier":
        repo_affiliations = []
        author_objs = Author.objects.filter(article_id=article.id)
        for author in author_objs:
            affiliation_objs = Affiliation.objects.filter(author_id=author.id)
            repo_affiliations.extend(affiliation_objs)
        additional_data["affiliations"] = len(repo_affiliations)
        additional_data["datasets"] = parse_datasets_from_xml(article, publisher)

    total_related_materials_dataset = sum(
        1 for rm in article.related_materials 
        if rm.related_material_type == "dataset"
    )
    total_related_materials_software = sum(
        1 for rm in article.related_materials 
        if rm.related_material_type == "software"
    )
    
    base_data.extend([total_related_materials_dataset, total_related_materials_software])
    
    if publisher in ["Springer", "APS"]:
        base_data.extend([
            additional_data["orcids"],
            additional_data["affiliations"],
            additional_data["rors"],
        ])
    elif publisher == "Hindawi":
        base_data.extend([
            additional_data["orcids"],
            additional_data["affiliations"],
            additional_data["datasets"],
        ])
    elif publisher == "OUP":
        base_data.extend([
            additional_data["orcids"],
            additional_data["affiliations"],
        ])
    elif publisher == "Elsevier":
        base_data.extend([
            additional_data["affiliations"],
            additional_data["datasets"],
        ])
    
    return base_data

def year_export(start_date=None, end_date=None, publisher_name=None):
    result_headers = get_publisher_headers(publisher_name)
    result_data = []

    search = ArticleDocument.search()

    if start_date or end_date:
        date_range = {}
        if start_date:
            date_range["gte"] = datetime.strptime(start_date, "%Y-%m-%d")
        if end_date:
            date_range["lte"] = datetime.strptime(end_date, "%Y-%m-%d")

        search = search.filter("range", publication_date=date_range)

    for article in search.scan():
        publisher = article.publication_info[0].publisher
        if publisher_name == publisher:
            result_data.append(process_article_data(article, publisher))

    ordered_fields = [
        "year",
        "journal",
        "doi",
        "publication date",
        "arxiv number",
        "primary arxiv category",
        "total number of authors",
        "total number of ORCIDs linked to the authors",
        "total number of non-unique affiliations",
        "total number of repository affiliations",
        "total number of RORs linked with the non-unique affiliations",
        "link to datasets",
        "total number of related materials, type dataset",
        "total number of related materials, type software",
    ]

    ordered_headers = [field for field in ordered_fields if field in result_headers]

    ordered_data = [
        [entry[result_headers.index(field)] for field in ordered_headers]
        for entry in result_data
    ]

    return {"header": ordered_headers, "data": ordered_data}

result = year_export("2024-10-01", "2024-12-31", "Elsevier")

with open("out.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(result["header"])
    writer.writerows(result["data"])
