#!/usr/bin/env python3
"""
Pub/Sub setup script
Creates topics and subscriptions needed for Debezium and Beam pipeline
"""

import os
import sys
import time
from google.cloud import pubsub_v1
from google.api_core.exceptions import AlreadyExists
import requests

# Configuration
PROJECT_ID = "streaming-project"
CDC_TOPICS = [
    "pgcdc.public.content",
    "pgcdc.public.engagement_events"
]

def wait_for_pubsub_ready(host, max_retries=30, delay=2):
    """Wait for Pub/Sub emulator to be ready"""
    print(f"Waiting for Pub/Sub emulator at {host}...")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(f"http://{host}/v1/projects/{PROJECT_ID}", timeout=5)
            if response.status_code in [200, 404]:  # 404 is fine, means emulator is up
                print("Pub/Sub emulator is ready!")
                return True
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries}: {e}")
        
        time.sleep(delay)
    
    print("Pub/Sub emulator not ready after maximum retries")
    return False

def create_topic(project_id, topic_name):
    """Create a Pub/Sub topic if it doesn't exist"""
    try:
        topic_path = f"projects/{project_id}/topics/{topic_name}"
        pubsub_v1.PublisherClient().create_topic(name=topic_path)
        print(f"Created topic: {topic_name}")
        return True
    except AlreadyExists:
        print(f"Topic already exists: {topic_name}")
        return True
    except Exception as e:
        print(f"Error creating topic {topic_name}: {e}")
        return False

def create_subscription(project_id, sub_name, topic_name):
    """Create a Pub/Sub subscription if it doesn't exist"""
    try:
        sub_path = f"projects/{project_id}/subscriptions/{sub_name}"
        topic_path = f"projects/{project_id}/topics/{topic_name}"
        pubsub_v1.SubscriberClient().create_subscription(
            name=sub_path, 
            topic=topic_path
        )
        print(f"Created subscription: {sub_name} -> {topic_name}")
        return True
    except AlreadyExists:
        print(f"Subscription already exists: {sub_name}")
        return True
    except Exception as e:
        print(f"Error creating subscription {sub_name}: {e}")
        return False

def setup_cdc_resources(project_id):
    """Set up all CDC topics and subscriptions"""
    success = True
    
    print(f"Setting up CDC Pub/Sub resources for project: {project_id}")
    
    for topic in CDC_TOPICS:
        # Create topic
        if not create_topic(project_id, topic):
            success = False
            continue
        
        # Create subscription for this topic
        table_name = topic.split(".")[-1]
        sub_name = f"cdc-{table_name}-sub"
        
        if not create_subscription(project_id, sub_name, topic):
            success = False
    
    return success

def main():
    """Main setup function"""
    print("Pub/Sub CDC Setup Starting")
    
    # Get Pub/Sub emulator host
    host = os.environ.get("PUBSUB_EMULATOR_HOST", "localhost:8085")
    print(f"Using Pub/Sub emulator: {host}")
    
    # Wait for emulator to be ready
    if not wait_for_pubsub_ready(host):
        print("Setup failed: Pub/Sub emulator not available")
        sys.exit(1)
    
    # Set up CDC resources
    if setup_cdc_resources(PROJECT_ID):
        print("Pub/Sub setup completed successfully!")
        sys.exit(0)
    else:
        print("Pub/Sub setup failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
