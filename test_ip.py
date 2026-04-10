from curl_cffi import requests

def main():
    url = "https://www.otodom.pl/pl/wyniki/sprzedaz/mieszkanie/malopolskie/krakow/krakow/krakow"
    print(f"Testing connection to: {url}")
    
    try:
        # We only need one request to see if the IP is banned
        response = requests.get(url, impersonate="chrome", timeout=15)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("SUCCESS! GitHub's IP is currently NOT blocked.")
            print(f"Downloaded HTML length: {len(response.text)}")
        elif response.status_code in [403, 405]:
            print("BLOCKED! Cloudflare detected the datacenter IP.")
        else:
            print(f"UNKNOWN BEHAVIOR. Status: {response.status_code}")
            
    except Exception as e:
        print(f"ERROR OCCURRED: {e}")

if __name__ == "__main__":
    main()