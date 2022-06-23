import time

from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
import os

def restart(vm_name, resource_group_name):
    VM_NAME = vm_name
    RESOURCE_GROUP_NAME = resource_group_name

    # Acquire a credential object using CLI-based authentication.
    credential = AzureCliCredential()

    # Retrieve subscription ID from environment variable.
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]

    compute_client = ComputeManagementClient(credential, subscription_id)


    print(f"{vm_name} VM Shutting down...")
    vm_stop = compute_client.virtual_machines.begin_power_off(RESOURCE_GROUP_NAME, VM_NAME)
    vm_stop.wait()
    print(f"{vm_name} VM shutdown complete")

    print(f"{vm_name} VM Starting...")
    vm_start = compute_client.virtual_machines.begin_start(RESOURCE_GROUP_NAME, VM_NAME)
    vm_start.wait()
    time.sleep(60)
    print(f"{vm_name} VM restart complete")
    #vm_restart = compute_client.virtual_machines.restart(RESOURCE_GROUP_NAME, VM_NAME)
    #vm_restart.wait()
    #vm_instance = compute_client.virtual_machines.get(RESOURCE_GROUP_NAME, VM_NAME)