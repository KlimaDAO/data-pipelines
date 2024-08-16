""" Raw ICR project data flow """
import json
import os

import pandas as pd
import requests
from prefect import task, flow
from prefect.blocks.system import Secret
import pycountry

SLUG = "fetch_icr_projects"


# Task Outline
# fetch_raw -> jsonify_meths, jsonify_projects -> load_meths -> load_projects -> enrich_project_content

def jsonify_record(i):
    i['methodologies'] = [{"_type": 'reference', "_ref": m.replace('.', '')} for m in i['methodologies']]
    i['id'] = {"_type": 'slug', "current": i['id']}

    em = []
    for desc, uri, filename, mimetype in i['externalMedia']:
        if filename is None or uri is None or uri == "":
            continue

        if not uri.startswith('https://'):
            uri = 'https://' + uri

        this_ef = {
            "_type": "externalFile", "filename": filename,
            "uri": uri, "mimetype": mimetype
        }

        if desc == '':
            this_ef['description'] = None
        else:
            this_ef['description'] = desc

        em.append(this_ef)
    i['externalMedia'] = em

    ed = []
    for desc, uri, filename, mimetype in i['externalDocuments']:
        if filename is None or uri is None or uri == "":
            continue

        if not uri.startswith('https://'):
            uri = 'https://' + uri

        this_ef = {
            "_type": "externalFile", "filename": filename,
            "uri": uri, "mimetype": mimetype
        }

        if desc == '':
            this_ef['description'] = None
        else:
            this_ef['description'] = desc

        ed.append(this_ef)
    i['externalDocuments'] = ed

def get_lat_long(record):
    lat = float(record['lat'])
    long = float(record['lng'])

    # format as geopoint type: https://www.sanity.io/docs/geopoint-type
    return {'_type': 'geopoint', 'lat': lat, 'lng': long}


@task()
def fetch_raw_icr_projects_task():
    # api_key_secret_block = Secret.load("icr-api-key")
    api_key = os.environ['ICR_API_KEY']

    # Access the stored secret
    # api_key = api_key_secret_block.get()

    # TODO: add pagination for when there are more than 50 projects
    proj_r = requests.get(
        'https://api.carbonregistry.com/v0/public/projects/list', params={"limit": 50, "page": 0},
        headers={"Authorization": f"Bearer {api_key}", "accept": "application/json", 'x-icr-api-version': '2023-06-16'}
    )

    proj_json = proj_r.json()['projects']

    return proj_json

@task()
def unpack_projects_task(proj_json):
    folder = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    meth_cats = pd.DataFrame(pd.read_json(os.path.join(folder, 'methodology-category-map.json'), typ='series'))[0]

    # Placeholder value
    meth_links = {}
    for r in open(os.path.join(folder, 'methodology_links.csv')):
        meth_id, link = r.split(',' , 1)
        meth_links[meth_id] = link.strip()

    methodologies = {}
    missing_locs = []

    def unpack_proj(proj):
        row = {}

        # Inject flat columns
        flat_cols = [
            'fullName', 'state', 'countryCode',
            'shortDescription', 'website'
            # Comment out unused columns for now until CMS supports
            # 'streetName', 'city', 'zip',
            # 'startDate', 'creditingPeriodStartDate', 'statusState', 'sequestrationPermanenceInYears',
            # 'updatedAt', 'methodologyDeviationDescription',
        ]
        for col in flat_cols:
            row[col] = proj[col]

        # Unpack nested fields
        # Methodology
        meth = proj['methodology']
        meth_id = meth['id'].replace(' ', '-')
        if not (meth['id'] in methodologies):
            this_meth = {}
            this_meth['name'] = meth['title']
            this_meth['category'] = meth_cats[meth_id]
            this_meth['link'] = meth_links[meth_id]
            methodologies[meth_id] = this_meth

        row['methodologies'] = [meth_id]

        # Static registry name for all projects from ICR
        row['registry'] = 'ICR'

        # Rename _id to registryProjectID (plain ID), will be concated with `Registry`
        row['registryProjectId'] = proj['carbonCredits'][0]['serialization'].split('-')[3]
        row['id'] = row['registry'] + '-' + row['registryProjectId']
        row['_id'] = row['id']

        # Convert country ID into short name
        iso = pycountry.countries.search_fuzzy(proj['countryCode'])[0].name
        row['country'] = iso

        # TODO: Project type (id, title, content)

        # TODO: Parse sector

        # Parse media ([] if no images, make sure not to include default image!)
        media = []
        for m in proj['media']:
            if not '.pdf' in m['fileName']:
                # Exclude PDFs since they don't render nicely as images
                media.append((m['description'], m['uri'], m['fileName'], m.get('mimetype')))
        row['externalMedia'] = media

        # Add document links
        docs = []
        for d in proj['documents']:
            docs.append((d['description'], d['uri'], d['fileName'], d['mimetype']))
        row['externalDocuments'] = docs

        # Parse 'geographicalRegion': {'id': 'euro', 'title': 'Europe'},
        row['region'] = proj['geographicalRegion']['title']

        # Parse 'geoLocation': {'lat': 64.14649144352953, 'lng': -21.93928744794059},
        # Make in to a point type for Sanity
        if proj['geoLocation']:
            row['lat'] = proj['geoLocation']['lat']
            row['lng'] = proj['geoLocation']['lng']
        else:
            print('Project ', row['id'], ' missing location! ', proj['geoLocation'])
            missing_locs.append(proj)


        # Parse SDGs out of 'otherBenefits': [{'title': 'SDG 6: Clean Water and Sanitation','checked': True,'benefitId': 5,'longTitle': 'Ensure availability and sustainable management of water and sanitation for all','description': ''},
        sdgs = []
        for sdg in proj['otherBenefits']:
            if 'SDG' in sdg['title']:
                sdgs.append(sdg['title'].split(' ')[1].split(':')[0])

        if sdgs:
            row['sdgs'] = sdgs
        else:
            row['sdgs'] = ['13'] # all carbon projects advance climate action

        # Add project URL based on base URL and project ID: https://www.carbonregistry.com/explore/projects/xyz123-abc456-...
        row['url'] = 'https://www.carbonregistry.com/explore/projects/' + proj['_id']

        # Add CORSIA boolean
        row['corsia'] = False

        # Figure out where CCBs are indicated (other than otherBenefits?)

        # Status (unused field right now)
        # row['projectStatus'] = proj['projectStatus']['title']

        return row

    proj = pd.DataFrame.from_records([unpack_proj(p) for p in proj_json if p['methodology']['id']])

    proj = proj.rename({
        'shortDescription': 'description',
        'fullName': 'name',
        'website': 'projectWebsite',
    }, axis=1)

    proj['_type'] = 'project'

    # Assign subcategories based on string matching
    proj.loc[proj.name.str.contains('(solar)|(photovoltaic)', case=False, regex=True), 'subcategory'] = 'solar'

    proj.loc[proj.name.str.contains('wind', case=False, regex=True), 'subcategory'] = 'wind'

    proj.loc[proj.name.str.contains('hydro', case=False, regex=True), 'subcategory'] = 'hydro'

    proj.loc[
        pd.notnull(proj.description) &
        proj.description.str.contains('afforestation', case=False, regex=True),
        'subcategory'
    ] = 'afforestation'

    meths_df = pd.DataFrame.from_dict(methodologies, orient='index')
    meths_df.index.name = 'id'

    meths_output = meths_df
    meths_output['_type'] = 'methodology'

    meths_output['_id'] = meths_output.index.str.replace('.', '')

    # Flag methodologies that are removals
    meths_output['isRemoval'] = False

    removal_meths = ['FCC', 'AR-AM0014']
    meths_output.loc[meths_output._id.isin(removal_meths), 'isRemoval'] = True

    meths_json = meths_output.to_json(lines=True, orient='records').split('\n')[:-1]
    meths_dicts = [json.loads(x) for x in meths_json]
    for i in meths_dicts:
        i['id'] = {"_type": 'slug', "current": i['_id']}

    # TODO: update to use proper Prefect outputting so this can be read downstream
    with open('./methodologies.ndjson', 'w') as f:
        for i in meths_dicts:
            f.write(json.dumps(i) + '\n')

    proj_json_post = proj.to_json(lines=True, orient='records').split('\n')[:-1]
    proj_dicts = [json.loads(x) for x in proj_json_post]
    for i in proj_dicts:
        jsonify_record(i)

        i['countryDetails'] = {"_type": 'reference', "_ref": i['countryCode']}

        if i['lat'] is not None and i['lng'] is not None:
            i['geolocation'] = get_lat_long(i)

        del i['lat']
        del i['lng']
        del i['countryCode']

    # TODO: update to use proper Prefect outputting so this can be read downstream
    with open('./icr-projects.ndjson', 'w') as f:
        for i in proj_dicts:
            f.write(json.dumps(i) + '\n')


# TODO: add validation task
# @task()
# def validate_raw_icr_task(df):
#     """Validates Verra data"""
#     utils.validate_against_latest_dataframe(SLUG, df)


@flow()
def fetch_projects_flow():
    """Fetches Verra data and stores it"""
    raw_proj = fetch_raw_icr_projects_task()
    unpack_projects_task(raw_proj)

    # TODO: add Sanity load logic
    # load_methodologies_task()
    # load_projects_task()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    fetch_projects_flow()
