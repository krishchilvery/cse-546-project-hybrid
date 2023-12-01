import requests

def check_openfaas_server(url):
    try:
        response = requests.get(url + "/system/functions", timeout=5)
        return response.status_code == 200
    except requests.exceptions.RequestException as err:
        print ("OOps: Something Else Happened",err)
    except requests.exceptions.HTTPError as errh:
        print ("Http Error:",errh)
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)

print(check_openfaas_server("http://192.168.49.2:31112"))