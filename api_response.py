from utils.token_manager import get_access_token
import requests

def call_invoice_api():
    token = get_access_token()
    url = "https://employmenthero.yourpayroll.com.au/api/v2/business/43551"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    print(response.json())

call_invoice_api()
