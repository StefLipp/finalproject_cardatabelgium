import io
import pandas as pd
import wikipedia as wp
import requests
import locale
from bs4 import BeautifulSoup

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


import pandas as pd
import io
import wikipedia as wp
from bs4 import BeautifulSoup

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    #read table from dutch wikipedia
    wp.set_lang("nl")

    html_wp = wp.page("Tabel_van_Belgische_gemeenten").html()

    # Parse HTML with BeautifulSoup
    soup = BeautifulSoup(html_wp, "html.parser")
    
    # Function to convert Belgian number formats
    def convert_belgian_number_format(value):
        if isinstance(value, str):
            value = value.replace('.', '')
            
            value = value.replace(',', '.')
            
            try:
                return str(float(value))  # Convert to float and then to string
            except ValueError:
                return value  # Return the original value if conversion fails
        return value

    # Clean up numeric formats in the HTML
    for table in soup.find_all("table"):
        for cell in table.find_all(["td", "th"]):
            if cell.string:
                new_value = convert_belgian_number_format(cell.string)
                cell.clear()
                cell.append(new_value)

    # Convert the modified HTML back to a string
    cleaned_html_wp = str(soup)

    # Read the cleaned HTML into a DataFrame
    try:
        wptable = pd.read_html(cleaned_html_wp)[1]
    except IndexError:
        wptable = pd.read_html(cleaned_html_wp)[0]

    # Convert to CSV and return the data
    wptable = wptable.to_csv(index=False)
    return pd.read_csv(io.StringIO(wptable))


