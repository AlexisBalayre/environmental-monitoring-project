import boto3
import csv
import time
from datetime import datetime, timedelta

# AWS Config
ec2_client = boto3.client("ec2")  # EC2 Client
autoscaling_client = boto3.client("autoscaling")  # Auto Scaling Group Client
cloudwatch_client = boto3.client("cloudwatch")  # CloudWatch Client
asg_name = "grafana-v2"  # Auto Scaling Group Name

# Store the previous states of the instances
previous_states = {}


def get_instance_states(ec2_client, instance_ids):
    """
    Retrives the state of the instances

    Args:
        ec2_client (boto3.client): EC2 Client
        instance_ids (list): List of instances IDs

    Returns:
        states (dict): Dictionary of instances IDs and their states
    """
    states = {}
    response = ec2_client.describe_instances(InstanceIds=instance_ids)
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            states[instance["InstanceId"]] = instance["State"]["Name"]
    return states


def get_instance_ids(asg_client, asg_name):
    """
    Retrives the instance IDs of the instances in the Auto Scaling Group

    Args:
        asg_client (boto3.client): Auto Scaling Group Client
        asg_name (str): Auto Scaling Group Name

    Returns:
        instance_ids (list): List of instances IDs
    """
    asg = asg_client.describe_auto_scaling_groups(AutoScalingGroupNames=[asg_name])
    return [
        instance["InstanceId"] for instance in asg["AutoScalingGroups"][0]["Instances"]
    ]


def get_ram_usage(cloudwatch_client, instance_id):
    """
    Retrives the average RAM usage of the instance

    Args:
        cloudwatch_client (boto3.client): CloudWatch Client
        instance_id (str): Instance ID

    Returns:
        ram_usage (float): Average RAM usage
    """
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=3)  # Period of 3 minutes
    metric_data = cloudwatch_client.get_metric_statistics(
        Namespace="CWAgent",
        MetricName="mem_used_percent",
        Dimensions=[{"Name": "InstanceId", "Value": instance_id}],
        StartTime=start_time,
        EndTime=end_time,
        Period=180,
        Statistics=["Average"],
    )
    if metric_data["Datapoints"]:
        return metric_data["Datapoints"][0]["Average"]
    return None


def get_cpu_usage(cloudwatch_client, instance_id):
    """
    Retrives the average CPU usage of the instance

    Args:
        cloudwatch_client (boto3.client): CloudWatch Client
        instance_id (str): Instance ID

    Returns:
        cpu_usage (float): Average CPU usage
    """
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(minutes=3)  # Period of 3 minutes
    metric_data = cloudwatch_client.get_metric_statistics(
        Namespace="CWAgent",
        MetricName="cpu_usage_user",
        Dimensions=[{"Name": "InstanceId", "Value": instance_id}],
        StartTime=start_time,
        EndTime=end_time,
        Period=180,
        Statistics=["Average"],
    )
    if metric_data["Datapoints"]:
        return metric_data["Datapoints"][0]["Average"]
    return None


# Main loop
while True:
    # Gets the instances IDs and their states in the Auto Scaling Group
    instance_ids = get_instance_ids(
        autoscaling_client, asg_name
    )  # Gets the instances IDs
    current_states = get_instance_states(
        ec2_client, instance_ids
    )  # Gets the instances states
    # Write the metrics to the CSV file
    with open("test/metrics.csv", "a", newline="") as file:
        writer = csv.writer(file)
        for instance_id in instance_ids:
            print("Instance ID: ", instance_id)
            ram_usage = get_ram_usage(
                cloudwatch_client, instance_id
            )  # Gets the RAM usage
            print("RAM usage: ", ram_usage)
            cpu_usage = get_cpu_usage(
                cloudwatch_client, instance_id
            )  # Gets the CPU usage
            print("CPU usage: ", cpu_usage)
            # Check if the state of the instance has changed
            state_changed = (
                instance_id in previous_states
                and previous_states[instance_id] != current_states[instance_id]
            )
            # Update the previous state
            previous_states[instance_id] = current_states[instance_id]
            print("Current state: ", current_states[instance_id])
            # Handle pending state
            if instance_id not in previous_states:
                current_states[instance_id] = "pending"
                ram_usage = None
                cpu_usage = None
            # Write the metrics to the CSV file
            if (
                current_states[instance_id] == "pending"
                or state_changed
                or (
                    current_states[instance_id] == "running"
                    and ram_usage is not None
                    and cpu_usage is not None
                )
            ):
                writer.writerow(
                    [
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        instance_id,
                        current_states[instance_id],
                        ram_usage,
                        cpu_usage,
                    ]
                )  # Write the metrics to the CSV file
                file.flush()  # Flush the buffer
    # Wait 1 second
    time.sleep(1)
