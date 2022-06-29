const api_token = '';           //set your API token
const api_host = 'api.mistsys.com';

const org_id = '';

const headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Token '+ api_token,
};

const user_metadata = {
    'time_zone': 'America/Los_Angeles',
    'first_name': '',
    'org_id': org_id
}

module.exports = {
    'api_host': api_host,
    'org_id': org_id,
    'headers': headers,
    'user_metadata': user_metadata
}