import json
from bs4 import BeautifulSoup

# Read the HTML you just downloaded
with open("../debug_page.html", "r", encoding="utf-8") as f:
    html = f.read()

soup = BeautifulSoup(html, "html.parser")

# Find the magic script tag
script_tag = soup.find("script", id="__NEXT_DATA__")
print(script_tag)

if script_tag:
    # Convert the text inside the tag into a Python dictionary
    data = json.loads(script_tag.string)

    # Save it nicely formatted so we can read it
    with open("../otodom_data.json", "w", encoding="utf-8") as out:
        json.dump(data, out, indent=4)

    print("Success! Saved perfectly formatted JSON to 'otodom_data.json'.")
else:
    print("Could not find __NEXT_DATA__ tag.")