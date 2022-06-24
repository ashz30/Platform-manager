# Import the needed credential and management objects from the libraries.
import time

from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.mgmt.compute import ComputeManagementClient
import os

#import restartVM
from VM import restartVM




def deleteVM(vm_name):
    VM_NAME = str(vm_name)
    VNET_NAME = "AshishEasowSandbox-vnet"
    SUBNET_NAME = "default"
    IP_NAME = VM_NAME + "-ip"
    NIC_NAME = VM_NAME + "-nic"
    DISK_NAME = VM_NAME + "-Disk"
    print(f"Deleting a virtual machine...some operations might take some time")
    print(f"Delete Request received for" , VM_NAME)

    # Acquire a credential object using CLI-based authentication.
    credential = AzureCliCredential()

    # Retrieve subscription ID from environment variable.
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]


    RESOURCE_GROUP_NAME = "AshishEasowSandbox"




    # Obtain the management object for networks
    network_client = NetworkManagementClient(credential, subscription_id)

    Subnet=network_client.subnets.get(RESOURCE_GROUP_NAME, VNET_NAME, SUBNET_NAME)
    print(Subnet.id)

    # Step 1: Deleting the virtual machine

    # Obtain the management object for virtual machines
    compute_client = ComputeManagementClient(credential, subscription_id)
    print(f"Deleting virtual machine {VM_NAME}; this operation might take a few minutes.")
    poller = compute_client.virtual_machines.begin_delete(RESOURCE_GROUP_NAME, VM_NAME)

    vm_result = poller.result()
    print(f"Deleted virtual machine {VM_NAME}")

    # Step 2: Deleting the disk

    poller = compute_client.disks.begin_delete(RESOURCE_GROUP_NAME,DISK_NAME)

    disk_result = poller.result()
    print(f"Deleted disk {DISK_NAME}..")

    # Step 2: Deleting the network interface client

    # Obtain the management object for networks
    network_client = NetworkManagementClient(credential, subscription_id)

    nic = network_client.network_interfaces.begin_delete(RESOURCE_GROUP_NAME,NIC_NAME)


    nic_result = nic.result()

    print(f"Deleted network interface client {NIC_NAME}")

    # Step 3: Deleting an IP address and wait for completion

    ip_add = network_client.public_ip_addresses.begin_delete(RESOURCE_GROUP_NAME,
                                                                       IP_NAME)


    ip_address_result = ip_add.result()
    print(f"Deleted public IP address {IP_NAME}")

    return VM_NAME


#deleteVM("RR2")