import requests


def getRRStatus(vm_name_ip, port):
    api_url = "http://" + vm_name_ip + ":" + port+ "/busy"
    #print("API :" , api_url)
    RR_STATUS = 'RR_DOWN'
    response = None
    try:
        response = requests.get(api_url)
    except:
        RR_STATUS = 'RR_DOWN'
        return RR_STATUS

    if(response.status_code==200):
        return "RR_UP"
    else:
        return "RR_DOWN"

#getRRStatus("rr1.eastus.cloudapp.azure.com")