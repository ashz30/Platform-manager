import requests


def getRRStatus(vm_name_ip, port):
    api_url = "http://" + vm_name_ip + ":" + port+ "/user%20name%20bpadminWS&password%20admin&busy"
    #print("API :" , api_url)
    RR_STATUS = 'RR_DOWN'
    response = None
    try:
        response = requests.get(api_url)
        #print("RR API Reponse :",response.text)
    except:
        RR_STATUS = 'RR_DOWN'
        return RR_STATUS

    if(response.text.__contains__('no')):
        return "RR_UP_NOT_BUSY"
    elif (response.text.__contains__('yes')):
        return "RR_UP_BUSY"
    else:
        return "RR_DOWN"

#print(getRRStatus("20.124.211.136","8181"))