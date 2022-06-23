# Import the needed credential and management objects from the libraries.
import time

from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute import ComputeManagementClient
import os

from VM import restartVM

from flask import Flask, jsonify, request

VM_NAME = "RR2"


app = Flask(__name__)

@app.route("/")
def default():
    return "Hello world>>!!"

@app.route('/provisionVM',methods=['GET'])

def provisionVM():
    vm_name = request.args.get('vm_name', None)  # VM name sent by client
    VM_NAME = str(vm_name)
    USERNAME = "bpadmin"
    PASSWORD = "Blueprism@123"
    VNET_NAME = "AshishEasowSandbox-vnet"
    SUBNET_NAME = "default"
    IP_NAME = VM_NAME + "-ip"
    IP_CONFIG_NAME = VM_NAME + "-ip-config"
    NIC_NAME = VM_NAME + "-nic"
    print(f"Provisioning a virtual machine...some operations might take some time")
    print(f"Request received for " , VM_NAME)

    # Acquire a credential object using CLI-based authentication.
    credential = AzureCliCredential()

    # Retrieve subscription ID from environment variable.
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]


    # Step 1: Provision a resource group

    # Obtain the management object for resources, using the credentials from the CLI login.
    resource_client = ResourceManagementClient(credential, subscription_id)

    # Constants we need in multiple places: the resource group name and the region
    # in which we provision resources. You can change these values however you want.
    RESOURCE_GROUP_NAME = "AshishEasowSandbox"
    LOCATION = "eastus"

    #for item in resource_client.resource_groups.list():
     #   print(item)

    # Provision the resource group.
    rg_result = resource_client.resource_groups.create_or_update(RESOURCE_GROUP_NAME,
        {
            "location": LOCATION
        }
    )

    print("Got Resource Group :" , rg_result)


    # For details on the previous code, see Example: Provision a resource group
    # at https://docs.microsoft.com/azure/developer/python/azure-sdk-example-resource-group


    # Step 2: provision a virtual network

    # A virtual machine requires a network interface client (NIC). A NIC requires
    # a virtual network and subnet along with an IP address. Therefore we must provision
    # these downstream components first, then provision the NIC, after which we
    # can provision the VM.

    # Network and IP address names


    # Obtain the management object for networks
    network_client = NetworkManagementClient(credential, subscription_id)


    # Query the virtual network and wait for completion
    #vnet = network_client.virtual_networks.get(virtual_network_name=VNET_NAME, resource_group_name=RESOURCE_GROUP_NAME)
    #print("Vnet Result", vnet)


    Subnet=network_client.subnets.get(RESOURCE_GROUP_NAME, VNET_NAME, SUBNET_NAME)
    print(Subnet.id)


    # Step 4: Provision an IP address and wait for completion
    ip_add = network_client.public_ip_addresses.begin_create_or_update(RESOURCE_GROUP_NAME,
        IP_NAME,
        {
            "location": LOCATION,
            "sku": { "name": "Standard" },
            "public_ip_allocation_method": "Static",
            "public_ip_address_version" : "IPV4"
        }
    )

    ip_address_result = ip_add.result()

    print(f"Provisioned public IP address {ip_address_result.name} with address {ip_address_result.ip_address}")

    #get applicable nsg for rules



    # Step 5: Provision the network interface client
    nic = network_client.network_interfaces.begin_create_or_update(RESOURCE_GROUP_NAME,
        NIC_NAME,
        {
            "location": LOCATION,
            "ip_configurations": [ {
                "name": IP_CONFIG_NAME,
                "subnet": { "id": Subnet.id },
                "public_ip_address": {"id": ip_address_result.id }
            }],
            "networkSecurityGroup":{"id":'/subscriptions/53bb9808-7dc2-4cbe-a5b5-cbc803b63489/resourceGroups/AshishEasowSandbox/providers/Microsoft.Network/networkSecurityGroups/RR2-nsg'}

        }
    )

    nic_result = nic.result()

    print(f"Provisioned network interface client {nic_result.name}")


    # Step 6: Provision the virtual machine

    # Obtain the management object for virtual machines
    compute_client = ComputeManagementClient(credential, subscription_id)



    print(f"Provisioning virtual machine {VM_NAME}; this operation might take a few minutes.")



    poller = compute_client.virtual_machines.begin_create_or_update(RESOURCE_GROUP_NAME, VM_NAME,
        {
            "location": LOCATION,
            "storageProfile": {
                "imageReference": {
                    "id": "/subscriptions/53bb9808-7dc2-4cbe-a5b5-cbc803b63489/resourceGroups/AshishEasowSandbox/providers/Microsoft.Compute/galleries/AshishResourceGallery/images/0.0.1/versions/0.0.3",
                },
                "osDisk": {
                    "caching": "ReadWrite",
                    "managedDisk": {
                        "storageAccountType": "Standard_LRS"
                    },
                    "name": VM_NAME+"-Disk",
                    "createOption": "FromImage"
                }
            },
            "hardware_profile": {
                "vm_size": "Standard_B2s"
            },
            "os_profile": {
                "computer_name": VM_NAME,
                "admin_username": USERNAME,
                "admin_password": PASSWORD
            },
            "network_profile": {
                "network_interfaces": [{
                    "id": nic_result.id,
                }]
            }
        }
    )

    vm_result = poller.result()

    print(f"Provisioned virtual machine {vm_result.name}, waiting for sometime")

    time.sleep(60)

    print(f"Restarting VM {VM_NAME}")
    RestartVM.restart(VM_NAME, RESOURCE_GROUP_NAME)
    return jsonify(vm_result.name)


if __name__ == "__main__":
    app.run(host='127.0.0.1', port=5000)