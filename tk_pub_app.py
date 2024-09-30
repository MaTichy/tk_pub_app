import os
import re
import subprocess
import threading
import time
import tkinter as tk
import traceback
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from tkinter import filedialog, messagebox, scrolledtext, simpledialog, ttk

import certifi
import pandas as pd
import requests
from bibtexparser.bibdatabase import BibDatabase
from bibtexparser.bparser import BibTexParser
from bibtexparser.bwriter import BibTexWriter
from bs4 import BeautifulSoup
from crossref_commons.iteration import iterate_publications_as_json
from rapidfuzz import fuzz
from requests.adapters import HTTPAdapter
from scholarly import scholarly
from urllib3.util import Retry

# Ensure the script uses certifi's CA bundle
os.environ['SSL_CERT_FILE'] = certifi.where()

# Initialize a thread pool for background tasks
executor = ThreadPoolExecutor(max_workers=5)

import logging

# Configure logging to file
logging.basicConfig(
    filename='publication_app.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class PublicationApp:
    def __init__(self, master):
        self.master = master
        master.title("Publication Viewer 2.6")
        master.geometry("1200x900")  # Increased height to accommodate the new Treeview

        # Frame for Year and Author Filtering
        self.frame_filter = ttk.Frame(master)
        self.frame_filter.pack(padx=10, pady=10, fill='x')

        # Year filter setup
        self.label_year = ttk.Label(self.frame_filter, text="Filter by Year (comma-separated):")
        self.label_year.pack(side=tk.LEFT, padx=(0, 10))
        self.entry_year = ttk.Entry(self.frame_filter, width=20)
        self.entry_year.pack(side=tk.LEFT, padx=(0, 10))

        # Author filter setup
        self.label_author_first = ttk.Label(self.frame_filter, text="First Name:")
        self.label_author_first.pack(side=tk.LEFT, padx=(0, 5))
        self.entry_author_first = ttk.Entry(self.frame_filter, width=15)
        self.entry_author_first.pack(side=tk.LEFT, padx=(0, 10))

        self.label_author_last = ttk.Label(self.frame_filter, text="Last Name:")
        self.label_author_last.pack(side=tk.LEFT, padx=(0, 5))
        self.entry_author_last = ttk.Entry(self.frame_filter, width=15)
        self.entry_author_last.pack(side=tk.LEFT, padx=(0, 10))

        # Apply filter button
        self.button_filter = ttk.Button(self.frame_filter, text="Apply Filter", command=self.filter_by_criteria)
        self.button_filter.pack(side=tk.LEFT)

        # Fetch and compare button
        self.fetch_button = ttk.Button(self.frame_filter, text="Fetch & Compare", command=self.fetch_and_compare)
        self.fetch_button.pack(side=tk.LEFT, padx=(10, 0))

        # Load BibTeX file button
        self.load_button = ttk.Button(self.frame_filter, text="Load BibTeX File", command=self.load_publications)
        self.load_button.pack(side=tk.LEFT, padx=(10, 0))

        # Autocomplete Single Publication button
        self.single_autocomplete_button = ttk.Button(self.frame_filter, text="Autocomplete Single Publication", command=self.autocomplete_single_publication)
        self.single_autocomplete_button.pack(side=tk.LEFT, padx=(10, 0))

        # Frame for source selection
        self.frame_sources = ttk.Frame(master)
        self.frame_sources.pack(padx=10, pady=5, fill='x')

        # Source selection checkboxes
        self.source_vars = {
            'Crossref': tk.BooleanVar(value=True),
            'Semantic Scholar': tk.BooleanVar(value=True),
            'Google Scholar': tk.BooleanVar(value=True),
            'DBLP': tk.BooleanVar(value=True),
        }

        for source, var in self.source_vars.items():
            cb = ttk.Checkbutton(self.frame_sources, text=source, variable=var)
            cb.pack(side=tk.LEFT, padx=(5, 5))

        # Publications display area using Treeview
        self.tree_frame = ttk.Frame(master)
        self.tree_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

        columns = ("Title", "Authors", "Year", "DOI")
        self.publication_tree = ttk.Treeview(self.tree_frame, columns=columns, show='headings', height=15)
        for col in columns:
            self.publication_tree.heading(col, text=col)
            self.publication_tree.column(col, width=200, anchor=tk.W)
        self.publication_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Add scrollbar to the Treeview
        tree_scroll = ttk.Scrollbar(self.tree_frame, orient="vertical", command=self.publication_tree.yview)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.publication_tree.configure(yscrollcommand=tree_scroll.set)

        # Progress text area
        self.progress_text = scrolledtext.ScrolledText(master, wrap=tk.WORD, width=100, height=5)
        self.progress_text.pack(pady=10, padx=10, fill=tk.BOTH)

        # Status bar for overall progress
        self.status_bar = ttk.Label(master, text="Ready", relief=tk.SUNKEN, anchor=tk.W)
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

        # Missing publications area
        self.missing_frame = ttk.Frame(master)
        self.missing_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

        self.missing_label = ttk.Label(self.missing_frame, text="Missing Publications:")
        self.missing_label.pack()

        self.missing_tree = ttk.Treeview(self.missing_frame, columns=columns, show='headings', height=10)
        for col in columns:
            self.missing_tree.heading(col, text=col)
            self.missing_tree.column(col, width=200, anchor=tk.W)
        self.missing_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Add scrollbar to the Missing Treeview
        missing_tree_scroll = ttk.Scrollbar(self.missing_frame, orient="vertical", command=self.missing_tree.yview)
        missing_tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.missing_tree.configure(yscrollcommand=missing_tree_scroll.set)

        # Extra publications area
        self.extra_frame = ttk.Frame(master)
        self.extra_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

        self.extra_label = ttk.Label(self.extra_frame, text="Extra Publications (Only in Local BibTeX):")
        self.extra_label.pack()

        self.extra_tree = ttk.Treeview(self.extra_frame, columns=columns, show='headings', height=10)
        for col in columns:
            self.extra_tree.heading(col, text=col)
            self.extra_tree.column(col, width=200, anchor=tk.W)
        self.extra_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Add scrollbar to the Extra Treeview
        extra_tree_scroll = ttk.Scrollbar(self.extra_frame, orient="vertical", command=self.extra_tree.yview)
        extra_tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.extra_tree.configure(yscrollcommand=extra_tree_scroll.set)

        # BibTeX text area for missing publications
        self.bibtex_label = ttk.Label(master, text="Missing Publications (BibTeX):")
        self.bibtex_label.pack()
        self.bibtex_text = scrolledtext.ScrolledText(master, wrap=tk.WORD, width=100, height=10)
        self.bibtex_text.pack(pady=10)

        # Statistics area
        self.statistics_label = ttk.Label(master, text="Statistics:")
        self.statistics_label.pack()
        self.statistics_text = scrolledtext.ScrolledText(master, wrap=tk.WORD, width=100, height=5)
        self.statistics_text.pack(pady=10)

        # Initialize publications
        self.publications = {}
        self.bibtex_file = None

    def load_publications(self):
        try:
            self.bibtex_file = filedialog.askopenfilename(
                title="Select BibTeX File",
                filetypes=[("BibTeX files", "*.bib"), ("All files", "*.*")]
            )
            if self.bibtex_file:
                with open(self.bibtex_file, 'r', encoding='utf-8') as file:
                    bibtex_str = file.read()
                bib_database = BibTexParser(common_strings=True).parse(bibtex_str)
                self.publications = self.organize_by_year(bib_database.entries)
                self.display_publications()  # Display all publications initially
                self.update_progress(f"Loaded publications from {self.bibtex_file}")
        except FileNotFoundError:
            messagebox.showerror("Error", "BibTeX file not found. Please check the file path.")
        except Exception as e:
            messagebox.showerror("Error", f"An error occurred: {str(e)}")
            logger.exception("Error in load_publications")

    def organize_by_year(self, entries):
        publications_by_year = defaultdict(list)
        for entry in entries:
            year = entry.get('year', 'Unknown')
            publications_by_year[year].append(entry)
        return publications_by_year

    def display_publications(self, years=None, first_name=None, last_name=None):
        # Clear the Treeview
        for item in self.publication_tree.get_children():
            self.publication_tree.delete(item)

        filtered_years = {year.strip() for year in (years or '').split(',')} if years else set()
        for pub_year, publications in sorted(self.publications.items()):
            if not years or pub_year in filtered_years:
                for publication in publications:
                    authors = publication.get('author', 'Unknown author')
                    if (not first_name or first_name.lower() in authors.lower()) and (not last_name or last_name.lower() in authors.lower()):
                        title = publication.get('title', 'No title available')
                        doi = publication.get('doi', '')
                        self.publication_tree.insert('', tk.END, values=(title, authors, pub_year, doi))

    def filter_by_criteria(self):
        years = self.entry_year.get()
        first_name = self.entry_author_first.get()
        last_name = self.entry_author_last.get()
        self.display_publications(years, first_name, last_name)

    def fetch_and_compare(self):
        # Get input for comparison
        first_name = self.entry_author_first.get()
        last_name = self.entry_author_last.get()
        years = [year.strip() for year in self.entry_year.get().split(',') if year.strip()]

        if not last_name or not years:
            messagebox.showerror("Error", "Please specify at least the last name and year(s).")
            return

        if not self.publications:
            messagebox.showerror("Error", "Please load a BibTeX file first.")
            return

        # Execute the crawling and comparison in a separate thread
        executor.submit(self.perform_crawl_and_compare, first_name, last_name, years)

    def update_progress(self, message):
        # Ensure thread-safe update for GUI components
        self.master.after(0, lambda: self.progress_text.insert(tk.END, message + "\n"))
        self.master.after(0, lambda: self.progress_text.see(tk.END))
        self.master.after(0, lambda: self.status_bar.config(text=message))
        logger.info(message)

    def perform_crawl_and_compare(self, first_name, last_name, years):
        try:
            self.update_progress("Fetching publications from the internet...")

            # Fetch publications from the internet for comparison
            selected_sources = [source for source, var in self.source_vars.items() if var.get()]
            if not selected_sources:
                self.update_progress("No sources selected. Please select at least one source.")
                self.master.after(0, lambda: messagebox.showerror("Error", "Please select at least one source to fetch publications."))
                return

            crawled_data = self.fetch_entries_by_author(first_name, last_name, selected_sources)
            if not crawled_data.empty:
                self.update_progress("Fetching complete. Now filtering by year...")

                # Save crawled publications to a CSV file
                self.save_crawled_publications_to_file(crawled_data)

                # Filter the crawled data by the specified years
                crawled_data['year'] = crawled_data['year'].astype(str)  # Ensure year is a string for comparison
                filtered_crawled_data = crawled_data[crawled_data['year'].isin(years)]

                if not filtered_crawled_data.empty:
                    self.update_progress("Found publications matching the year criteria.")

                    # Proceed with the filtered crawled data
                    completed_crawled_data = filtered_crawled_data

                    # Compare completed crawled data with local data
                    self.update_progress("Comparing local and crawled publications...")
                    local_bibtex_data = self.convert_to_dataframe(self.publications, years, first_name, last_name)

                    missing_pubs, extra_pubs = self.compare_publications(local_bibtex_data, completed_crawled_data)

                    self.update_progress("Displaying missing publications...")
                    self.display_missing_publications(missing_pubs)
                    self.update_progress("Displaying extra publications...")
                    self.display_extra_publications(extra_pubs)
                    self.update_progress("Generating BibTeX for missing publications...")
                    self.display_missing_bibtex(missing_pubs)

                    # Update statistics
                    self.update_progress("Updating statistics...")
                    self.update_statistics(
                        len(local_bibtex_data),
                        len(completed_crawled_data),
                        len(local_bibtex_data) - len(extra_pubs),
                        len(missing_pubs),
                        len(extra_pubs)
                    )
                    self.update_progress("Comparison and display completed.")
                else:
                    self.update_progress("No publications found matching the year criteria.")
                    self.master.after(0, lambda: messagebox.showinfo("Info", "No publications found for the given years."))
            else:
                self.update_progress("No publications found from the internet for the specified criteria.")
                self.master.after(0, lambda: messagebox.showinfo("Info", "No crawled data found for the given criteria."))
        except Exception as e:
            self.update_progress(f"An unexpected error occurred: {str(e)}")
            logger.exception("Unexpected error in perform_crawl_and_compare")

    def fetch_entries_by_author(self, first_name, last_name, selected_sources):
        author = f"{first_name} {last_name}".strip()
        publications = []

        try:
            self.update_progress(f"Fetching entries for author: {author}")

            if 'Crossref' in selected_sources:
                try:
                    self.update_progress("Fetching from Crossref...")
                    crossref_pubs = self.fetch_from_crossref(first_name, last_name)
                    publications.extend(crossref_pubs)
                    self.update_progress(f"Crossref fetch complete. Found {len(crossref_pubs)} publications.")
                except Exception as e:
                    self.update_progress(f"Error fetching from Crossref: {str(e)}")
                    logger.exception("Exception in fetch_from_crossref")

            if 'Semantic Scholar' in selected_sources:
                try:
                    self.update_progress("Fetching from Semantic Scholar...")
                    semantic_scholar_pubs = self.fetch_from_semantic_scholar(first_name, last_name)
                    publications.extend(semantic_scholar_pubs)
                    self.update_progress(f"Semantic Scholar fetch complete. Found {len(semantic_scholar_pubs)} publications.")
                except Exception as e:
                    self.update_progress(f"Error fetching from Semantic Scholar: {str(e)}")
                    logger.exception("Exception in fetch_from_semantic_scholar")

            if 'Google Scholar' in selected_sources:
                try:
                    self.update_progress("Fetching from Google Scholar...")
                    google_scholar_pubs = self.fetch_from_google_scholar(first_name, last_name)
                    publications.extend(google_scholar_pubs)
                    self.update_progress(f"Google Scholar fetch complete. Found {len(google_scholar_pubs)} publications.")
                except Exception as e:
                    self.update_progress(f"Error fetching from Google Scholar: {str(e)}")
                    logger.exception("Exception in fetch_from_google_scholar")

            if 'DBLP' in selected_sources:
                try:
                    self.update_progress("Fetching from DBLP...")
                    dblp_pubs = self.fetch_from_dblp(first_name, last_name)
                    publications.extend(dblp_pubs)
                    self.update_progress(f"DBLP fetch complete. Found {len(dblp_pubs)} publications.")
                except Exception as e:
                    self.update_progress(f"Error fetching from DBLP: {str(e)}")
                    logger.exception("Exception in fetch_from_dblp")

            time.sleep(2)  # Optional: wait to ensure all processes are complete
        except Exception as e:
            self.update_progress(f"Error fetching entries for {first_name} {last_name}: {str(e)}")
            logger.exception("Error in fetch_entries_by_author")

        unique_publications = self.remove_duplicates(publications)

        self.update_progress(f"Finished fetching entries. Total unique publications found: {len(unique_publications)}")

        if not unique_publications:
            self.update_progress("No publications found. Check if the APIs are accessible and the author name is correct.")
        else:
            self.save_crawled_publications_to_file(pd.DataFrame(unique_publications))

        return pd.DataFrame(unique_publications)

    def fetch_from_crossref(self, first_name, last_name, max_results=1000):
        publications = []
        query = f"{first_name} {last_name}"

        filter = {
            'from-pub-date': '2000-01-01',
        }
        queries = {'query.author': query}

        try:
            for item in iterate_publications_as_json(max_results=max_results, filter=filter, queries=queries):
                if self.author_match(f"{first_name} {last_name}", item.get('author', [])):
                    pub = self.parse_crossref_item(item)
                    publications.append(pub)
        except Exception as e:
            self.update_progress(f"Error fetching from Crossref: {str(e)}")
            logger.exception("Exception in fetch_from_crossref")

        return publications

    def fetch_from_semantic_scholar(self, first_name, last_name):
        publications = []
        query = f"{first_name} {last_name}"

        try:
            # Initialize the Semantic Scholar API client
            api_url = 'https://api.semanticscholar.org/graph/v1/author/search'
            params = {
                'query': query,
                'fields': 'papers.title,papers.year,papers.authors,papers.doi,papers.externalIds',
                'limit': 1
            }
            response = requests.get(api_url, params=params)
            data = response.json()

            if 'data' in data and data['data']:
                author_id = data['data'][0]['authorId']
                self.update_progress(f"Found Semantic Scholar author ID: {author_id}")

                # Fetch papers by author ID
                papers_url = f'https://api.semanticscholar.org/graph/v1/author/{author_id}/papers'
                papers_params = {
                    'fields': 'title,year,authors,doi,externalIds',
                    'limit': 1000
                }
                papers_response = requests.get(papers_url, params=papers_params)
                papers_data = papers_response.json()

                if 'data' in papers_data:
                    for paper in papers_data['data']:
                        # Check if the author matches
                        if any(self.author_match(query, f"{a.get('name', '')}") for a in paper.get('authors', [])):
                            pub = {
                                'title': paper.get('title', ''),
                                'year': str(paper.get('year', '')),
                                'author': ', '.join([a.get('name', '') for a in paper.get('authors', [])]),
                                'doi': paper.get('doi', ''),
                                'ENTRYTYPE': 'article',
                                'ID': paper.get('doi', f"SS_{paper.get('paperId', '')}")
                            }
                            publications.append(pub)
        except Exception as e:
            self.update_progress(f"Error fetching from Semantic Scholar: {str(e)}")
            logger.exception("Exception in fetch_from_semantic_scholar")

        return publications

    def fetch_from_google_scholar(self, first_name, last_name):
        publications = []
        query = f"{first_name} {last_name}"

        try:
            search_query = scholarly.search_author(query)
            author = next(search_query, None)
            if author:
                author = scholarly.fill(author)
                for pub in author['publications']:
                    if 'bib' in pub:
                        bib = pub['bib']
                        # Check if the author matches
                        if self.author_match(query, bib.get('author', '')):
                            pub_data = {
                                'title': bib.get('title', ''),
                                'year': str(bib.get('pub_year', '')),
                                'author': bib.get('author', ''),
                                'doi': bib.get('doi', ''),
                                'ENTRYTYPE': bib.get('ENTRYTYPE', 'article'),
                                'ID': bib.get('doi', f"GS_{bib.get('title', '')}")
                            }
                            publications.append(pub_data)
        except Exception as e:
            self.update_progress(f"Error fetching from Google Scholar: {str(e)}")
            logger.exception("Exception in fetch_from_google_scholar")

        return publications

    def fetch_from_dblp(self, first_name, last_name):
        publications = []
        query = f"{first_name} {last_name}"
        try:
            url = f'https://dblp.org/search/publ/api?q=author%3A{first_name}%20{last_name}&format=json&h=1000'
            response = requests.get(url)
            data = response.json()

            hits = data.get('result', {}).get('hits', {}).get('hit', [])
            for hit in hits:
                info = hit.get('info', {})
                authors = info.get('authors', {}).get('author', [])
                if isinstance(authors, dict):
                    authors = [authors]
                authors_list = [a.get('text', '') for a in authors]
                # Check if the author matches
                if any(self.author_match(query, a) for a in authors_list):
                    pub_data = {
                        'title': info.get('title', ''),
                        'year': str(info.get('year', '')),
                        'author': ', '.join(authors_list),
                        'doi': info.get('doi', ''),
                        'ENTRYTYPE': 'article',
                        'ID': info.get('doi', f"DBLP_{info.get('key', '')}")
                    }
                    publications.append(pub_data)
        except Exception as e:
            self.update_progress(f"Error fetching from DBLP: {str(e)}")
            logger.exception("Exception in fetch_from_dblp")

        return publications

    def author_match(self, query_author, pub_authors):
        """
        Checks if the query_author matches any of the authors in pub_authors.

        The matching is strict:
        - The last names must match exactly after normalization.
        - The first name must match exactly or by initial.

        Args:
            query_author (str): The full name of the author to match (e.g., "Max Mustermann").
            pub_authors (list or str): The list of authors from the publication.

        Returns:
            bool: True if a match is found, False otherwise.
        """
        # Normalize the query author's name
        normalized_query = self.normalize_author(query_author)
        query_parts = normalized_query.split()
        if len(query_parts) < 2:
            return False  # Not enough information to perform matching
        query_first_name = query_parts[0]
        query_last_name = ' '.join(query_parts[1:])

        # Handle the publication authors
        if isinstance(pub_authors, list):
            authors_list = [f"{a.get('given', '')} {a.get('family', '')}".strip() for a in pub_authors]
        elif isinstance(pub_authors, str):
            authors_list = [name.strip() for name in pub_authors.split(',')]
        else:
            authors_list = []

        # Normalize and check each author in the publication
        for author in authors_list:
            normalized_author = self.normalize_author(author)
            author_parts = normalized_author.split()
            if len(author_parts) < 2:
                continue  # Skip if the author's name is incomplete
            author_first_name = author_parts[0]
            author_last_name = ' '.join(author_parts[1:])

            # Check if last names match exactly
            if query_last_name != author_last_name:
                continue

            # Check if first names match exactly or by initial
            if (query_first_name == author_first_name) or (query_first_name[0] == author_first_name[0] and len(author_first_name) == 2 and author_first_name[1] == '.'):
                return True  # Match found

        return False  # No match found

    def normalize_author(self, author):
        if isinstance(author, list):
            author = ' '.join([f"{a.get('given', '')} {a.get('family', '')}" for a in author])
        return self.normalize_text(author)

    def normalize_text(self, text):
        if not isinstance(text, str):
            logger.warning(f"normalize_text received non-string input: {type(text)}")
            text = str(text)
        # Remove accents and convert to lowercase
        text = ''.join(c for c in unicodedata.normalize('NFKD', text) if not unicodedata.combining(c))
        text = text.lower()
        # Remove non-alphanumeric characters except for periods
        text = re.sub(r'[^\w.\s]', '', text)
        return text.strip()

    def parse_crossref_item(self, item):
        pub_authors = []
        if 'author' in item:
            for a in item['author']:
                given = a.get('given', '')
                family = a.get('family', '')
                if given and family:
                    pub_authors.append(f"{given} {family}")
        pub_authors = ', '.join(pub_authors)

        # Extract and format 'published-print'
        published_print = item.get('published-print', {})
        if 'date-parts' in published_print:
            date_parts = published_print['date-parts'][0]
            published_print_str = '-'.join(map(str, date_parts))
        else:
            published_print_str = ''

        # Extract and format 'published-online'
        published_online = item.get('published-online', {})
        if 'date-parts' in published_online:
            date_parts = published_online['date-parts'][0]
            published_online_str = '-'.join(map(str, date_parts))
        else:
            published_online_str = ''

        # Extract the title string for ID generation
        title_list = item.get('title', [''])
        title = title_list[0] if isinstance(title_list, list) and title_list else 'No Title'

        # Generate a unique ID using DOI if available, else use a sanitized title
        doi = item.get('DOI', '')
        if doi:
            unique_id = doi
        else:
            # Sanitize the title to create a valid BibTeX ID
            sanitized_title = re.sub(r'\W+', '', title).lower()
            unique_id = f"key{hash(sanitized_title)}"

        # Extract additional fields with proper handling
        pub = {
            'author': pub_authors,
            'year': str(
                item.get('published-print', {}).get('date-parts', [[None]])[0][0] or
                item.get('published-online', {}).get('date-parts', [[None]])[0][0] or ''
            ),
            'title': title,
            'doi': doi,
            'container-title': item.get('container-title', [''])[0],
            'publisher': item.get('publisher', ''),
            'abstract': item.get('abstract', ''),
            'ISSN': ', '.join(item.get('ISSN', [])),
            'ISBN': ', '.join(item.get('ISBN', [])),
            'URL': item.get('URL', ''),
            'type': item.get('type', ''),
            'language': item.get('language', ''),
            'page': item.get('page', ''),
            'volume': item.get('volume', ''),
            'issue': item.get('issue', ''),
            'published-print': published_print_str,
            'published-online': published_online_str,
            'reference-count': str(item.get('reference-count', '')),  # Convert to string
            'subject': ', '.join(item.get('subject', [])),
            'ENTRYTYPE': item.get('type', 'article'),  # Default to 'article' if not specified
            'ID': unique_id  # Use the generated unique ID
        }
        return pub

    def remove_duplicates(self, publications):
        unique_pubs = []
        seen_titles = set()
        for pub in publications:
            normalized_title = self.normalize_text(pub['title'])
            if normalized_title not in seen_titles:
                seen_titles.add(normalized_title)
                unique_pubs.append(pub)
        return unique_pubs

    def save_crawled_publications_to_file(self, df):
        try:
            df.to_csv('crawled_publications.csv', index=False)
            self.update_progress("Crawled publications saved to 'crawled_publications.csv'")
        except Exception as e:
            self.update_progress(f"Error saving crawled publications: {str(e)}")
            logger.exception("Exception in save_crawled_publications_to_file")

    def convert_to_dataframe(self, publications, years, first_name, last_name):
        data = []
        for year, pubs in publications.items():
            if year in years:
                for pub in pubs:
                    if self.author_match(f"{first_name} {last_name}", pub.get('author', '')):
                        data.append(pub)
        return pd.DataFrame(data)

    def compare_publications(self, local_data, crawled_data):
        def normalize_text(text):
            if not isinstance(text, str):
                text = str(text)
            text = re.sub(r'{\\[a-z]{1,2}}', '', text)
            text = re.sub(r'{\\\w+\s*}', '', text)
            text = re.sub(r'[^\w\s]', '', text.lower())
            return text

        def is_similar(pub1, pub2, doi_threshold=90, title_threshold=80):
            doi1 = normalize_text(pub1.get('doi', ''))
            doi2 = normalize_text(pub2.get('doi', ''))
            title1 = normalize_text(pub1.get('title', ''))
            title2 = normalize_text(pub2.get('title', ''))

            # First, compare DOIs
            doi_similarity = fuzz.ratio(doi1, doi2)
            if doi_similarity >= doi_threshold and doi1:
                return True

            # If DOIs are not similar, compare titles
            title_similarity = fuzz.ratio(title1, title2)
            return title_similarity >= title_threshold

        missing_pubs = []
        extra_pubs = []

        for _, crawled_pub in crawled_data.iterrows():
            if not any(is_similar(crawled_pub, local_pub) for _, local_pub in local_data.iterrows()):
                missing_pubs.append(crawled_pub)

        for _, local_pub in local_data.iterrows():
            if not any(is_similar(local_pub, crawled_pub) for _, crawled_pub in crawled_data.iterrows()):
                extra_pubs.append(local_pub)

        # Convert lists to DataFrames
        missing_pubs_df = pd.DataFrame(missing_pubs)
        extra_pubs_df = pd.DataFrame(extra_pubs)

        # Remove duplicates in missing publications
        if not missing_pubs_df.empty:
            missing_pubs_df = missing_pubs_df.drop_duplicates(subset=['title', 'doi'])

        return missing_pubs_df, extra_pubs_df

    def display_missing_publications(self, missing_pubs):
        self.master.after(0, lambda: self._display_missing_publications(missing_pubs))

    def _display_missing_publications(self, missing_pubs):
        # Clear the Treeview
        for item in self.missing_tree.get_children():
            self.missing_tree.delete(item)

        for _, pub in missing_pubs.iterrows():
            title = pub.get('title', 'No title')
            authors = pub.get('author', 'Unknown author')
            year = pub.get('year', 'Unknown')
            doi = pub.get('doi', '')
            self.missing_tree.insert('', tk.END, values=(title, authors, year, doi))

    def display_extra_publications(self, extra_pubs):
        self.master.after(0, lambda: self._display_extra_publications(extra_pubs))

    def _display_extra_publications(self, extra_pubs):
        # Clear the Extra Treeview
        for item in self.extra_tree.get_children():
            self.extra_tree.delete(item)

        for _, pub in extra_pubs.iterrows():
            title = pub.get('title', 'No title')
            authors = pub.get('author', 'Unknown author')
            year = pub.get('year', 'Unknown')
            doi = pub.get('doi', '')
            self.extra_tree.insert('', tk.END, values=(title, authors, year, doi))

    def update_statistics(self, local_count, crawled_count, common_count, missing_count, extra_count):
        self.master.after(0, lambda: self._update_statistics(local_count, crawled_count, common_count, missing_count, extra_count))

    def _update_statistics(self, local_count, crawled_count, common_count, missing_count, extra_count):
        stats = f"Local publications: {local_count}\n"
        stats += f"Crawled publications: {crawled_count}\n"
        stats += f"Common publications: {common_count}\n"
        stats += f"Missing publications: {missing_count}\n"
        stats += f"Extra publications: {extra_count}\n"
        self.statistics_text.delete(1.0, tk.END)
        self.statistics_text.insert(tk.END, stats)

    def display_missing_bibtex(self, missing_pubs):
        self.master.after(0, lambda: self._display_missing_bibtex(missing_pubs))

    def _display_missing_bibtex(self, missing_pubs):
        writer = BibTexWriter()
        writer.indent = '    '
        bib_db = BibDatabase()
        # Convert DataFrame to list of dictionaries
        entries = missing_pubs.to_dict('records')
        # Ensure all fields in entries are strings
        for entry in entries:
            for key in entry:
                if not isinstance(entry[key], str):
                    entry[key] = str(entry[key])
        bib_db.entries = entries
        bib_db.comments = []       # Initialize as empty list
        bib_db.preambles = []      # Initialize as empty list
        bib_db.strings = {}        # Initialize as empty dict
        try:
            bibtex_str = writer.write(bib_db)
            self.bibtex_text.delete(1.0, tk.END)
            self.bibtex_text.insert(tk.END, bibtex_str)
        except KeyError as e:
            self.update_progress(f"BibTeX writing error: Missing key {e}")
            logger.exception("KeyError in _display_missing_bibtex")
        except Exception as e:
            self.update_progress(f"Unexpected error writing BibTeX: {str(e)}")
            logger.exception("Unexpected error in _display_missing_bibtex")

    def write_bibtex(self, publications, filename):
        try:
            # Convert the list of publication dictionaries into a DataFrame
            df = pd.DataFrame(publications)

            # Replace all NaN values with empty strings
            df.fillna('', inplace=True)

            # Convert the DataFrame back to a list of dictionaries
            publications = df.to_dict('records')

            # Ensure all fields in entries are strings
            for entry in publications:
                for key in entry:
                    if not isinstance(entry[key], str):
                        entry[key] = str(entry[key])

            writer = BibTexWriter()
            writer.indent = '    '
            bib_db = BibDatabase()
            bib_db.entries = publications
            bib_db.comments = []       # Initialize as empty list
            bib_db.preambles = []      # Initialize as empty list
            bib_db.strings = {}        # Initialize as empty dict

            # Validate and sanitize each entry
            for entry in bib_db.entries:
                # Ensure 'ENTRYTYPE' and 'ID' are present
                if 'ENTRYTYPE' not in entry:
                    raise KeyError(f"Missing 'ENTRYTYPE' in entry: {entry.get('ID', 'Unknown ID')}")
                if 'ID' not in entry:
                    raise KeyError(f"Missing 'ID' in entry: {entry.get('ENTRYTYPE', 'Unknown ENTRYTYPE')}")

                # Optional: Remove fields that are empty to clean up the BibTeX entries
                keys_to_remove = [key for key, value in entry.items() if value == '']
                for key in keys_to_remove:
                    del entry[key]

            with open(filename, 'w', encoding='utf-8') as bibtex_file:
                bibtex_file.write(writer.write(bib_db))
            self.update_progress(f"Wrote {len(publications)} entries to {filename}")
            with open(filename, 'r', encoding='utf-8') as f:
                self.update_progress(f"First 500 characters of {filename}:")
                self.update_progress(f.read(500))
        except KeyError as e:
            self.update_progress(f"BibTeX writing error: Missing key {e}")
            logger.exception("KeyError in write_bibtex")
        except Exception as e:
            self.update_progress(f"Unexpected error writing BibTeX: {str(e)}")
            logger.exception("Unexpected error in write_bibtex")

    def autocomplete_single_publication(self):
        # Open a dialog to get DOI or title from the user
        input_data = simpledialog.askstring("Input", "Enter DOI or Title of the publication:")
        if not input_data:
            return

        # Create a temporary BibTeX entry
        temp_entry = {
            'ENTRYTYPE': 'article',
            'ID': 'temp_entry',
            'doi' if input_data.startswith('10.') else 'title': input_data
        }
        temp_bib_file = 'temp_single_pub.bib'
        output_bib_file = 'completed_single_pub.bib'

        # Write the temp entry to a BibTeX file
        self.write_bibtex([temp_entry], temp_bib_file)

        # Run btac on the temp BibTeX file
        if self.run_bibtex_autocomplete(temp_bib_file, output_bib_file):
            # Read the output and display to the user
            completed_data = self.read_bibtex(output_bib_file)
            if not completed_data.empty:
                # Display the autocompleted BibTeX entry
                self.display_single_bibtex(completed_data)
            else:
                self.update_progress("No data found in the autocompleted file.")
                messagebox.showinfo("Info", "No data found in the autocompleted file.")
        else:
            self.update_progress("BibTeX autocomplete failed.")
            messagebox.showerror("Error", "BibTeX autocomplete failed.")

    def run_bibtex_autocomplete(self, input_file, output_file):
        self.update_progress(f"Running btac on {input_file}...")
        command = ['btac', input_file, '-o', output_file]
        try:
            # Start the btac subprocess
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            # Read and log stdout in real-time
            while True:
                output = process.stdout.readline()
                if output:
                    self.update_progress(output.strip())
                if process.poll() is not None:
                    # Capture remaining output
                    remaining = process.stdout.read()
                    if remaining:
                        self.update_progress(remaining.strip())
                    break

            # Capture and log stderr
            stderr = process.stderr.read()
            if stderr:
                self.update_progress(f"btac Errors:\n{stderr.strip()}")

            if process.returncode == 0:
                self.update_progress(f"btac completed successfully. Output saved to {output_file}.")
                return True
            else:
                self.update_progress(f"btac exited with return code {process.returncode}.")
                return False
        except subprocess.TimeoutExpired:
            self.update_progress("btac process timed out.")
            return False
        except Exception as e:
            self.update_progress(f"Unexpected error running btac: {str(e)}")
            logger.exception("Unexpected exception in run_bibtex_autocomplete")
            return False

    def read_bibtex(self, filename):
        try:
            with open(filename, 'r', encoding='utf-8') as bibtex_file:
                parser = BibTexParser()
                bib_database = parser.parse_file(bibtex_file)
            return pd.DataFrame(bib_database.entries)
        except Exception as e:
            self.update_progress(f"Error reading BibTeX file {filename}: {str(e)}")
            logger.exception("Exception in read_bibtex")
            return pd.DataFrame()

    def display_single_bibtex(self, bibtex_data):
        writer = BibTexWriter()
        writer.indent = '    '
        bib_db = BibDatabase()
        bib_db.entries = bibtex_data.to_dict('records')
        try:
            bibtex_str = writer.write(bib_db)
            # Show the BibTeX in a message box
            self.master.after(0, lambda: messagebox.showinfo("Autocompleted BibTeX", bibtex_str))
        except Exception as e:
            self.update_progress(f"Error displaying BibTeX: {str(e)}")
            logger.exception("Error in display_single_bibtex")

if __name__ == "__main__":
    root = tk.Tk()
    app = PublicationApp(root)
    root.mainloop()
