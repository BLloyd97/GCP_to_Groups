import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import os
import json

# Get the service account key from the environment variable
gcp_key = os.getenv('GCP_KEY')

if gcp_key is None:
    raise ValueError("GCP_KEY environment variable is not set.")

# Load the JSON key into a dictionary
try:
    credentials_dict = json.loads(gcp_key)  # Ensure it's in dictionary form
except json.JSONDecodeError as e:
    raise ValueError("GCP_KEY is not a valid JSON string.") from e

# Create a BigQuery client from the credentials dictionary
bq_client = bigquery.Client.from_service_account_info(credentials_dict)

# Query the BigQuery table
bq_query_job = bq_client.query("""
    SELECT email FROM demsnmsp.commons.data_updates_listserv limit 5
""")

# Wait for the query to complete and convert the result to a Pandas DataFrame
bq_result = bq_query_job.result()
df_bq = bq_result.to_dataframe()

# Print the retrieved data (optional)
print("Data retrieved from BigQuery:")
print(df_bq)

# Define your credentials and scopes for Google Admin SDK
scopes = ['https://www.googleapis.com/auth/admin.directory.group']
admin_credentials = os.getenv('DPNM_GOOGLE_ADMIN_KEY')

# Build the Admin SDK service
service = build('admin', 'directory_v1', credentials=admin_credentials)

# Google Group email
group_email = 'all-van-users-updates@nmdemocrats.org'
owner_email = 'brian@nmdemocrats.org'

# Function to remove all members except for the specified owner
def remove_all_members(group_email, owner_email):
    try:
        # List all members in the group
        results = service.members().list(groupKey=group_email).execute()
        members = results.get('members', [])

        if not members:
            print(f"No members found in {group_email}.")
            return
        
        for member in members:
            member_email = member['email']
            if member_email != owner_email:  # Do not remove the owner
                try:
                    # Remove the member
                    service.members().delete(groupKey=group_email, memberKey=member_email).execute()
                    print(f"Removed {member_email} from {group_email}.")
                except Exception as e:
                    print(f"Failed to remove {member_email}: {e}")
            else:
                print(f"Owner {owner_email} will not be removed.")

    except Exception as e:
        print(f"Failed to list members of {group_email}: {e}")

# Call the function to remove all members except for the owner
remove_all_members(group_email, owner_email)

# Add the owner if not already a member
try:
    service.members().insert(
        groupKey=group_email,
        body={
            'email': owner_email,
            'role': 'OWNER'  # Set role as 'OWNER'
        }
    ).execute()
    print(f"Ensured {owner_email} is an Owner of the group {group_email}.")
except Exception as e:
    print(f"Failed to add owner {owner_email}: {e}")

# Add new members from the BigQuery result to the Google Group
for index, row in df_bq.iterrows():
    email = row['email']  # Adjust the column name if it differs
    try:
        # Add the user to the group
        service.members().insert(
            groupKey=group_email,
            body={
                'email': email,
                'role': 'MEMBER'  # You can set role as 'MEMBER' or 'OWNER' or 'MANAGER'
            }
        ).execute()
        print(f"Added {email} to the group {group_email}.")
    except Exception as e:
        print(f"Failed to add {email}: {e}")
