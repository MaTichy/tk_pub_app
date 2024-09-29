import json
from collections import defaultdict

import requests
from bibtexparser.bparser import BibTexParser


def fetch_publications(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def parse_bibtex(bibtex_str):
    parser = BibTexParser(common_strings=True)
    bib_database = parser.parse(bibtex_str)
    return bib_database.entries

def organize_by_year(entries):
    publications_by_year = defaultdict(list)
    for entry in entries:
        year = entry.get('year', 'Unbekannt')
        publications_by_year[year].append(entry)
    return publications_by_year

def save_bibtex_data(bibtex_str, filename='TK_Publikationen_Komplett.bib'):
    with open(filename, 'w', encoding='utf-8') as file:
        file.write(bibtex_str)
    print(f"Daten erfolgreich in {filename} gespeichert.")

def cache_data(data, filename='publications_cache.json'):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)

# URL für die API-Anfrage
url = "https://tubiblio.ulb.tu-darmstadt.de/cgi/search/archive/advanced/export_tubiblio_BibTeX.bib?dataset=archive&screen=Search&_action_export=1&output=BibTeX&exp=0%7C1%7C-date%2Fcreators_name%2Ftitle%7Carchive%7C-%7Cdivisions%3Adivisions%3AANY%3AEQ%3Afb20_tk%7C-%7Ceprint_status%3Aeprint_status%3AANY%3AEQ%3Aarchive%7Cmetadata_visibility%3Ametadata_visibility%3AANY%3AEQ%3Ashow&n=&cache=8102631"

# Funktionen verwenden, um Daten abzurufen und zu speichern
bibtex_data = fetch_publications(url)
entries = parse_bibtex(bibtex_data)
publications_by_year = organize_by_year(entries)
cache_data(publications_by_year)  # Speichert das Dictionary als JSON
save_bibtex_data(bibtex_data)  # Speichert die rohen BibTeX-Daten
