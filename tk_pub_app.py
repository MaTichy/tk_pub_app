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
        master.title("Publication Viewer 2.4")
        master.geometry("1200x800")

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

        columns = ("Title", "Authors", "Year", "DOI")
        self.missing_tree = ttk.Treeview(self.missing_frame, columns=columns, show='headings', height=10)
        for col in columns:
            self.missing_tree.heading(col, text=col)
            self.missing_tree.column(col, width=200, anchor=tk.W)
        self.missing_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        # Add scrollbar to the Missing Treeview
        missing_tree_scroll = ttk.Scrollbar(self.missing_frame, orient="vertical", command=self.missing_tree.yview)
        missing_tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.missing_tree.configure(yscrollcommand=missing_tree_scroll.set)

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

    def author_match(self, query_author, pub_authors):
        # Ensure that query_author has both first and last name
        normalized_query = self.normalize_author(query_author)
        parts = normalized_query.split()
        if len(parts) < 2:
            return False
        first_name, last_name = parts[0], ' '.join(parts[1:])

        if isinstance(pub_authors, list):
            # Assuming each item in the list is a dict with 'given' and 'family'
            pub_authors_str = ', '.join([f"{a.get('given', '')} {a.get('family', '')}" for a in pub_authors])
        elif isinstance(pub_authors, str):
            pub_authors_str = pub_authors
        else:
            pub_authors_str = ''

        pub_authors_norm = self.normalize_author(pub_authors_str)

        return last_name in pub_authors_norm and (first_name in pub_authors_norm or first_name[0] + '.' in pub_authors_norm)

    def normalize_author(self, author):
        if isinstance(author, list):
            author = ' '.join([f"{a.get('given', '')} {a.get('family', '')}" for a in author])
        return self.normalize_text(author)

    def normalize_text(self, text):
        if not isinstance(text, str):
            logger.warning(f"normalize_text received non-string input: {type(text)}")
            text = str(text)
        return ''.join(c for c in unicodedata.normalize('NFKD', text.lower())
                       if not unicodedata.combining(c))

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

    def fetch_from_semantic_scholar(self, first_name, last_name):
        base_url = "https://api.semanticscholar.org/graph/v1/author/search"
        publications = []

        session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
        session.mount('https://', HTTPAdapter(max_retries=retries))

        try:
            params = {
                'query': f"{first_name} {last_name}",
                'fields': 'name,papers.title,papers.year,papers.authors,papers.externalIds,papers.venue'
            }
            response = session.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()

            if 'data' in data:
                for author in data['data']:
                    if self.exact_author_match(f"{first_name} {last_name}", author['name']):
                        for paper in author.get('papers', []):
                            pub = {
                                'author': ', '.join([a['name'] for a in paper.get('authors', [])]),
                                'year': str(paper.get('year', 'Unknown')),
                                'title': paper.get('title', ''),
                                'doi': paper.get('externalIds', {}).get('DOI', ''),
                                'container-title': paper.get('venue', ''),
                                'publisher': '',
                                'ENTRYTYPE': 'article',  # Default type
                                'ID': paper.get('externalIds', {}).get('DOI', f"key{hash(paper.get('title', ''))}")
                            }
                            publications.append(pub)
                        break  # Assuming exact match, stop after first match
        except Exception as e:
            self.update_progress(f"Error fetching from Semantic Scholar: {str(e)}")
            logger.exception("Exception in fetch_from_semantic_scholar")

        return publications

    def fetch_from_google_scholar(self, first_name, last_name):
        publications = []
        try:
            self.update_progress(f"Searching for author: {first_name} {last_name} in Google Scholar...")
            search_query = scholarly.search_author(f"{first_name} {last_name}")
            authors = list(search_query)

            def get_author_name(author):
                if isinstance(author, dict):
                    return author.get('name', '')
                return getattr(author, 'name', '')

            exact_match = next((a for a in authors if self.exact_author_match(f"{first_name} {last_name}", get_author_name(a))), None)

            if exact_match:
                self.update_progress("Author found. Fetching publications...")
                author = scholarly.fill(exact_match, sections=['basics', 'publications'])

                for i, pub in enumerate(author.get('publications', [])[:20]):
                    if i % 5 == 0:
                        self.update_progress(f"Fetched {i} publications...")

                    try:
                        filled_pub = self.threaded_timeout(scholarly.fill, (pub,), timeout=30)
                        if filled_pub:
                            bib = filled_pub.get('bib', {})
                            publications.append({
                                'author': ', '.join(bib.get('author', '').split(' and ')),
                                'year': bib.get('pub_year', 'Unknown'),
                                'title': bib.get('title', ''),
                                'doi': filled_pub.get('pub_url', ''),
                                'container-title': bib.get('journal', ''),
                                'publisher': '',
                                'ENTRYTYPE': 'article',  # Default type
                                'ID': bib.get('pub_year', f"key{hash(bib.get('title', ''))}")  # Use pub_year as ID or generate unique key
                            })
                        else:
                            self.update_progress(f"Timeout while fetching details for publication {i+1}")
                    except Exception as e:
                        self.update_progress(f"Error fetching details for publication {i+1}: {str(e)}")
                        logger.exception(f"Exception fetching publication {i+1} details")

                    time.sleep(1)
            else:
                self.update_progress(f"No exact author match found in Google Scholar for: {first_name} {last_name}")
        except Exception as e:
            self.update_progress(f"Google Scholar API Request Error for {first_name} {last_name}: {str(e)}")
            logger.exception("Exception in fetch_from_google_scholar")

        self.update_progress(f"Google Scholar fetch complete. Found {len(publications)} publications.")
        return publications

    def fetch_from_dblp(self, first_name, last_name):
        publications = []
        searches = [
            f"{first_name} {last_name}",
            f"{first_name[0]}. {last_name}",
            last_name
        ]

        for search_query in searches:
            try:
                self.update_progress(f"Searching DBLP for: {search_query}")
                search_url = f"https://dblp.org/search/publ/api?q={search_query}&format=xml"
                response = requests.get(search_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'xml')
                hits = soup.find_all('hit')

                for hit in hits:
                    info = hit.find('info')
                    if info:
                        authors = info.find('authors')
                        if authors:
                            author_names = [author.text for author in authors.find_all('author')]
                            author_str = ', '.join(author_names)
                        else:
                            author_str = ''

                        title = info.find('title')
                        year = info.find('year')
                        venue = info.find('venue')

                        # Format 'published-online' if available
                        published_online = info.find('published-online')
                        if published_online:
                            published_online_str = published_online.text
                        else:
                            published_online_str = 'Unknown'

                        publication = {
                            'author': author_str,
                            'year': year.text if year else 'Unknown',
                            'title': title.text if title else '',
                            'doi': '',
                            'container-title': venue.text if venue else '',
                            'publisher': '',
                            'ENTRYTYPE': 'article',  # Default type
                            'ID': f"key{hash(title.text if title else '')}"
                        }

                        if self.author_match(f"{first_name} {last_name}", publication['author']):
                            publications.append(publication)
            except Exception as e:
                self.update_progress(f"Error fetching from DBLP for {search_query}: {str(e)}")
                logger.exception("Exception in fetch_from_dblp")

        self.update_progress(f"DBLP fetch complete. Found {len(publications)} publications.")
        return publications

    def threaded_timeout(self, func, args=(), kwargs={}, timeout=None):
        result = [None]

        def worker():
            try:
                result[0] = func(*args, **kwargs)
            except Exception as e:
                result[0] = e

        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            return None
        if isinstance(result[0], Exception):
            raise result[0]
        return result[0]

    def exact_author_match(self, query_name, author_name):
        query_parts = set(query_name.lower().split())
        author_parts = set(author_name.lower().split())
        return query_parts == author_parts

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
        # Clear any existing entries in the publication tree
        for item in self.publication_tree.get_children():
            self.publication_tree.delete(item)

        for _, pub in extra_pubs.iterrows():
            title = pub.get('title', 'No title')
            authors = pub.get('author', 'Unknown author')
            year = pub.get('year', 'Unknown')
            doi = pub.get('doi', '')
            self.publication_tree.insert('', tk.END, values=(title, authors, year, doi))

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
                # Display the completed BibTeX entry
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
