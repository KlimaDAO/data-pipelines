


task()
def populate_icr_project_content():
    # Set the API endpoint URL
    dataset = 'production'
    url = f'https://l6of5nwi.api.sanity.io/v2021-10-21/data/mutate/{dataset}'

    # Load bearer token
    # token = os.environ['SANITY_TOKEN']
    token = userdata.get('sanity_token')

    type_name = 'projectContent'

    def create_project_if_not_exists(row):
        # Set the document ID and patch data
        project_id = row['_id']
        id_value = 'content-' + project_id
        patch_data = {'mutations': [
            {
                'createIfNotExists': {
                    '_id': id_value,
                    'project': {"_type": "reference", "_ref": project_id},
                    '_type': type_name,
                }
            }
        ]}

        # Set the headers and authentication token
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {token}'
        }

        # Make the patch request
        response = requests.post(url, headers=headers, json=patch_data)

        # Check the response status code
        if response.status_code == 200:
            print(f'Patch request for {project_id} successful!')
        else:
            print(f'Patch request for {project_id} failed with status code {response.status_code}')

        return response
