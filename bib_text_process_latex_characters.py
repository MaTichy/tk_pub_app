import re


def preprocess_bib_file(input_file, output_file):
    # Dictionary of LaTeX encodings and their Unicode counterparts
    conversions = {
        r'{\\"u}': 'ü', r'{\\"o}': 'ö', r'{\\"a}': 'ä',
        r'{\\"U}': 'Ü', r'{\\"O}': 'Ö', r'{\\"A}': 'Ä',
        r'{\\"s}': 'ß', r'{\'a}': 'a', r'{\'e}': 'e',
        r'{\'i}': 'i', r'{\'o}': 'o', r'{\'u}': 'u',
        r'{\\ss}': 'ß', r'{\\i}': 'ı', r'{\\j}': 'ȷ',
        r'{\\o}': 'ø', r'{\\l}': 'ł', r'{\\n}': 'ñ',
        r'{\\r}': 'ř', r'{\\v}': 'v', r'{\\u}': 'u',
        r'{\\v}': 'v', r'{\\H}': 'H', r'{\\c}': 'c',
        r'{\\k}': 'k', r'{\\j}': 'j', r'{\\v}': 'v',
        r'{\\v}': 'v', r'{\\v}': 'v', r'{\\v}': 'v',
        r'{\\v}': 'v', r'{\\v}': 'v', r'{\\v}': 'v',
        r'{\\v}': 'v', r'{\\v}': 'v', r'{\\v}': 'v',
        r'{\\v}': 'v', r'{\\v}': 'v', r'{\\v}': 'v',
        r'{\\v}': 'v', r'{\\v}': 'v', r'{\\v}': 'v',
        r'{\\v}': 'v', r'{\\v}': 'v', r'{\\v}': 'v',
        r'{\\v}': 'v', r'{\\v}': 'v', r'{\\v}': 'v',
        r'{\\v}': 'v', r'{\\v}': 'v', r'{\\v}': 'v',
        
        # More conversions can be added here
    }

    try:
        with open(input_file, 'r', encoding='utf-8') as file:
            content = file.read()

        # Use regex to replace all LaTeX encodings
        for latex, unicode in conversions.items():
            content = re.sub(latex, unicode, content)

        with open(output_file, 'w', encoding='utf-8') as file:
            file.write(content)

        print("File has been successfully preprocessed and saved as:", output_file)
    except FileNotFoundError:
        print("The specified file does not exist.")
    except Exception as e:
        print("An error occurred:", e)

# Usage
input_bib_file = './TK_Publikationen_Komplett.bib'
output_bib_file = './processed_bibtex_file_latex_characters.bib'
preprocess_bib_file(input_bib_file, output_bib_file)
