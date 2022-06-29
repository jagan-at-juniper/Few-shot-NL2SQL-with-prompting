const Alexa = require('ask-sdk-core');

const post_request = require(__dirname + '/post_request.js');
const get_request = require(__dirname + '/get_request.js');

const auth = require(__dirname + '/auth.js');

const configs = require(__dirname + '/config.js');

var query = 'Hello';        // deafault user query

/********************************
* For getting name and email address of the user
*********************************/

var alexaApiData = {
    'apiAccessToken': '',
    'apiEndpoint': ''
}

const EMAIL_PERMISSION = 'alexa::profile:email:read';
const NAME_PERMISSION = 'alexa::profile:given_name:read';


var userName = '';
var userEmail = '';


//To handle initial Launch request
const LaunchRequestHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'LaunchRequest';
    },
    async handle(handlerInput) {
        let speakOutput = 'This is the default message';
        
        
        /***************** 
         * To GET name and email of every user upon launching the Skills
         *****************/
        
        //alexa's api token
        alexaApiData.apiAccessToken = handlerInput.requestEnvelope.context.System.apiAccessToken;
        
        //alexa's endpoint to request info
        alexaApiData.apiEndpoint = handlerInput.requestEnvelope.context.System.apiEndpoint;
        alexaApiData.apiEndpoint = alexaApiData.apiEndpoint.slice(8);
        
        //reformatting the api_token
        const alexa_api_token = 'Bearer ' + alexaApiData.apiAccessToken;
        
        //creating options for sending request for name and email
        const optionsForName = {
            hostname: alexaApiData.apiEndpoint,
            path: '/v2/accounts/~current/settings/Profile.givenName',
            port: 443,
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': alexa_api_token
            }
        }
        const optionsForEmail = {
            hostname: alexaApiData.apiEndpoint,
            path: '/v2/accounts/~current/settings/Profile.email',
            port: 443,
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': alexa_api_token
            }
        }
        
        //sending get request to retrieve the name
        await get_request(optionsForName)
        .then((response) => {
            const data = JSON.parse(response);
            userName = data;
        })
        .catch((err) => {
            console.log(`ERROR: ${err.message}`);
            speakOutput = `Some Error Occured: ${err.message}. Please check your permissions`;
            
            return handlerInput.responseBuilder
                .speak(speakOutput)
                .withAskForPermissionsConsentCard([EMAIL_PERMISSION, NAME_PERMISSION])  
                
        });
        
        //for email
        await get_request(optionsForEmail)
        .then((response) => {
            const data = JSON.parse(response);
            userEmail = data;
        })
        .catch((err) => {
            console.log(`ERROR: ${err.message}`);
            speakOutput = `Some Error Occured: ${err.message}. Please check your permissions`;
            
            return handlerInput.responseBuilder
                .speak(speakOutput)
                .withAskForPermissionsConsentCard([EMAIL_PERMISSION, NAME_PERMISSION])  

        });
        /*********************
         * End of requesting name and email
         *********************/
         
        //retrieving mist api token and org id of user from auth.js
        auth.forEach((obj) => {
            if (obj.email === userEmail){
                speakOutput = 'Found you credentials';
                configs.headers.Authorization = 'Token '+ obj.token;
                configs.org_id = obj.org_id;
                configs.user_metadata.first_name = userName;
                configs.user_metadata.org_id = obj.org_id;
                console.log("\n Org ID: " + obj.org_id + " Token: " + obj.token);
            }
        })
        
        if (configs.headers.Authorization === 'Token '){
            return handlerInput.responseBuilder
            .speak("Permission error! You can't use the skill.")
            .getResponse();
        }

        
        //setting launch output message
        speakOutput = `Hi ${userName}. Welcome to Mist AI by Juniper. How may I help you?`;

        return handlerInput.responseBuilder
            .speak(speakOutput)
            .reprompt('Do you need any assistance?')
            .getResponse();
    }
};


//To Handle user query for Marvis
const QueryIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && Alexa.getIntentName(handlerInput.requestEnvelope) === 'QueryIntent';
    },
    
    async handle(handlerInput) {
        
    let speakOutput = 'This is the default message';

    //To Verify our Query slot isn't empty
    if (handlerInput.requestEnvelope.request.intent.slots.query.value){ 
        query =  handlerInput.requestEnvelope.request.intent.slots.query.value;
    }
    else{
        return handlerInput.responseBuilder
      .speak('Sorry! Unable to fetch your query')
      .getResponse();
    }
    
    //options to create our post request to marvis
    const options = {
        method: 'POST',
        hostname: configs.api_host,
        path: '/api/v1/labs/orgs/' + configs.org_id + '/chatbot_converse',
        port: 443,
        headers: configs.headers
    }
    
    //sending post request 
    await post_request(options, query)
    .then((response) => {
        const rcvd_data = JSON.parse(response);

        //jhgbkhbjbhbb
        
        
        // TO HANDLE DIFFERENT TYPES OF RESPONSES
        // Later, will create a seperate function to iterate through whole response and print out only necessary details
        for (let k = 0; k < rcvd_data.data.length; k++) {
            let item = rcvd_data.data[k];

            if (item.type ===  'text'){
                speakOutput = `${item.response[0]} \n `;
            }

            else if (item.type === 'options'){
                let speech;

                if (item.response.length === 1){
                    speech = `${item.response[0].title}. ${item.response[0].description}`;
                    speakOutput = `${speakOutput + speech}`;

                    item.response[0].response.forEach((i_item, idx) => {
                        if (i_item.type === 'text'){
                            speech = `\n${(idx+1)}. ${i_item.response[0]}`;
                            speakOutput = `${speakOutput + speech}`;
                        }
                    })
                }
                else{
                    item.response.forEach((item_2, idx) => {
                        speech = `\n${(idx+1)}. ${item_2.title}. ${item_2.description}`;
                        speakOutput = `${speakOutput + speech}`;
                    })
                } 
                break;      
            }

            else if (item.type === 'entityList'){
                speakOutput = `${speakOutput} Found multiple devices with this name. Please be more specific about the device name. For example, say: `;
                speakOutput = `${speakOutput} ${item.response[0].list[0].display.phrase}`;
                break;
            }

            else if (item.type === 'table'){
                item.response[0].item_list.forEach((li_item, i) => {
                    let name = li_item.Name;
                    if ((name.split(":").length) !== 6){   //ignoring device with mac as name
                        speakOutput = ` ${speakOutput}${li_item.Name}, `;
                    }
                })                
                break;
            }
        }
        //...........
    })
    .catch((err) => {
        console.log(`ERROR: ${err.message}`);
        speakOutput = `Some Error Occured: ${err.message}`
    });

    return handlerInput.responseBuilder
      .speak(speakOutput)
      .reprompt('Do you need any other assistance?')
      .getResponse();
  },
};

//To handle unhappy users
const UnhappyUsersIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && Alexa.getIntentName(handlerInput.requestEnvelope) === 'UnhappyUsersIntent';
    },
    
    async handle(handlerInput) {
        
    let speakOutput = 'This is the default message';

    //options to create our post request to marvis
    const options = {
        method: 'POST',
        hostname: configs.api_host,
        path: '/api/v1/labs/orgs/' + configs.org_id + '/chatbot_converse',
        port: 443,
        headers: configs.headers
    }
    
    
    var site_name = '';
    
    if (handlerInput.requestEnvelope.request.intent.slots.site_name.hasOwnProperty('value')){
        if (handlerInput.requestEnvelope.request.intent.slots.site_name.resolutions.resolutionsPerAuthority[0].status.code === "ER_SUCCESS_MATCH"){
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.site_name.resolutions.resolutionsPerAuthority[0].values[0].value.name;
        }
        else{
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.site_name.value;
        }
    }
    
    query = 'unhappy users' + site_name;
    
    //sending post request 
    await post_request(options, query)
    .then((response) => {
        const rcvd_data = JSON.parse(response);
    
        for (let k = 0; k < rcvd_data.data.length; k++) {
            let item = rcvd_data.data[k];
            
            if (item.type ===  'text'){
                speakOutput = `${item.response[0]}.\n`;
            }

            else if (item.type === 'entityList'){
                let speech;

                item.response[0].list.forEach((item_2, i) => {
                    if (i !== 0 && i === item.response[0].list.length - 1) {
                        // last record
                        speakOutput = `${speakOutput}and ${item_2.title}.`;
                    } 
                    else{
                        // first and middle record
                        speakOutput = `${speakOutput}${item_2.title}, `;
                    } 
                })
            }
        }
    })
    .catch((err) => {
        console.log(`ERROR: ${err.message}`);
        speakOutput = `Some Error Occured: ${err.message}`
    });

    var cardTitle ;
    
   
    cardTitle = 'Unhappy Users at ' + site_name.slice(4);
    

    return handlerInput.responseBuilder
      .speak(speakOutput)
      .withSimpleCard(cardTitle, speakOutput)
      .reprompt('Do you need any other assistance?')
      .getResponse();
  },
};

const UnhappySwitchIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && Alexa.getIntentName(handlerInput.requestEnvelope) === 'UnhappySwitchIntent';
    },
    
    async handle(handlerInput) {
        
    let speakOutput = 'This is the default message';

    //options to create our post request to marvis
    const options = {
        method: 'POST',
        hostname: configs.api_host,
        path: '/api/v1/labs/orgs/' + configs.org_id + '/chatbot_converse',
        port: 443,
        headers: configs.headers
    }
    
    
    var site_name = '';
    
    if (handlerInput.requestEnvelope.request.intent.slots.site_name.hasOwnProperty('value')){
        if (handlerInput.requestEnvelope.request.intent.slots.site_name.resolutions.resolutionsPerAuthority[0].status.code === "ER_SUCCESS_MATCH"){
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.site_name.resolutions.resolutionsPerAuthority[0].values[0].value.name;
        }
        else{
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.site_name.value;
        }
    }
    
    query = 'unhappy switch' + site_name;
    
    //sending post request 
    await post_request(options, query)
    .then((response) => {
        const rcvd_data = JSON.parse(response);
    
        for (let k = 0; k < rcvd_data.data.length; k++) {
            let item = rcvd_data.data[k];
            
            if (item.type ===  'text'){
                speakOutput = `${item.response[0]}.\n`;
            }

            else if (item.type === 'entityList'){
                let speech;

                item.response[0].list.forEach((item_2, i) => {
                    if (i !== 0 && i === item.response[0].list.length - 1) {
                        // last record
                        speakOutput = `${speakOutput}and ${item_2.title}.`;
                    } 
                    else{
                        // first and middle record
                        speakOutput = `${speakOutput}${item_2.title}, `;
                    } 
                })
            }
        }
    })
    .catch((err) => {
        console.log(`ERROR: ${err.message}`);
        speakOutput = `Some Error Occured: ${err.message}`
    });

    var cardTitle ;
    
    if (site_name === ''){
        cardTitle = 'Unhappy Switches in your Organization';
    }
    else{
        cardTitle = 'Unhappy Switches at ' + site_name.slice(4);
    }
    

    return handlerInput.responseBuilder
      .speak(speakOutput)
      .withSimpleCard(cardTitle, speakOutput)
      .reprompt('Do you need any other assistance?')
      .getResponse();
  },
};

const UnhappyGatewayIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && Alexa.getIntentName(handlerInput.requestEnvelope) === 'UnhappyGatewayIntent';
    },
    
    async handle(handlerInput) {
        
    let speakOutput = 'This is the default message';

    //options to create our post request to marvis
    const options = {
        method: 'POST',
        hostname: configs.api_host,
        path: '/api/v1/labs/orgs/' + configs.org_id + '/chatbot_converse',
        port: 443,
        headers: configs.headers
    }
    
    
    var site_name = '';
    
    if (handlerInput.requestEnvelope.request.intent.slots.site_name.hasOwnProperty('value')){
        if (handlerInput.requestEnvelope.request.intent.slots.site_name.resolutions.resolutionsPerAuthority[0].status.code === "ER_SUCCESS_MATCH"){
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.site_name.resolutions.resolutionsPerAuthority[0].values[0].value.name;
        }
        else{
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.site_name.value;
        }
    }
    
    query = 'unhappy gateway' + site_name;
    
    //sending post request 
    await post_request(options, query)
    .then((response) => {
        const rcvd_data = JSON.parse(response);
    
        for (let k = 0; k < rcvd_data.data.length; k++) {
            let item = rcvd_data.data[k];
            
            if (item.type ===  'text'){
                speakOutput = `${item.response[0]}.\n`;
            }

            else if (item.type === 'entityList'){
                let speech;

                item.response[0].list.forEach((item_2, i) => {
                    if (i !== 0 && i === item.response[0].list.length - 1) {
                        // last record
                        speakOutput = `${speakOutput}and ${item_2.title}.`;
                    } 
                    else{
                        // first and middle record
                        speakOutput = `${speakOutput}${item_2.title}, `;
                    } 
                })
            }
        }
    })
    .catch((err) => {
        console.log(`ERROR: ${err.message}`);
        speakOutput = `Some Error Occured: ${err.message}`
    });

    var cardTitle ;
    
    if (site_name === ''){
        cardTitle = 'Unhappy Gateways in your Organization';
    }
    else{
        cardTitle = 'Unhappy Gateways at ' + site_name.slice(4);
    }
    

    return handlerInput.responseBuilder
      .speak(speakOutput)
      .withSimpleCard(cardTitle, speakOutput)
      .reprompt('Do you need any other assistance?')
      .getResponse();
  },
};

const UnhappyApsIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && Alexa.getIntentName(handlerInput.requestEnvelope) === 'UnhappyApsIntent';
    },
    
    async handle(handlerInput) {
        
    let speakOutput = 'This is the default message';

    //options to create our post request to marvis
    const options = {
        method: 'POST',
        hostname: configs.api_host,
        path: '/api/v1/labs/orgs/' + configs.org_id + '/chatbot_converse',
        port: 443,
        headers: configs.headers
    }
    
    
    var site_name = '';
    
    if (handlerInput.requestEnvelope.request.intent.slots.site_name.hasOwnProperty('value')){
        if (handlerInput.requestEnvelope.request.intent.slots.site_name.resolutions.resolutionsPerAuthority[0].status.code === "ER_SUCCESS_MATCH"){
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.site_name.resolutions.resolutionsPerAuthority[0].values[0].value.name;
        }
        else{
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.site_name.value;
        }
    }
    
    query = 'unhappy aps' + site_name;
    
    //sending post request 
    await post_request(options, query)
    .then((response) => {
        const rcvd_data = JSON.parse(response);
    
        for (let k = 0; k < rcvd_data.data.length; k++) {
            let item = rcvd_data.data[k];
            
            if (item.type ===  'text'){
                speakOutput = `${item.response[0]}.\n`;
            }

            else if (item.type === 'entityList'){
                let speech;

                item.response[0].list.forEach((item_2, i) => {
                    if (i !== 0 && i === item.response[0].list.length - 1) {
                        // last record
                        speakOutput = `${speakOutput}and ${item_2.title}.`;
                    } 
                    else{
                        // first and middle record
                        speakOutput = `${speakOutput}${item_2.title}, `;
                    } 
                })
            }
        }
    })
    .catch((err) => {
        console.log(`ERROR: ${err.message}`);
        speakOutput = `Some Error Occured: ${err.message}`
    });

    var cardTitle ;
    
    if (site_name === ''){
        cardTitle = 'Unhappy Access Points in your Organization';
    }
    else{
        cardTitle = 'Unhappy Access Points at ' + site_name.slice(4);
    }
    

    return handlerInput.responseBuilder
      .speak(speakOutput)
      .withSimpleCard(cardTitle, speakOutput)
      .reprompt('Do you need any other assistance?')
      .getResponse();
  },
};

//To handle list APs
const ListApsIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && Alexa.getIntentName(handlerInput.requestEnvelope) === 'ListApsIntent';
    },
    
    async handle(handlerInput) {
        
    let speakOutput = 'This is the default message';

    //options to create our post request to marvis
    const options = {
        method: 'POST',
        hostname: configs.api_host,
        path: '/api/v1/labs/orgs/' + configs.org_id + '/chatbot_converse',
        port: 443,
        headers: configs.headers
    }
    
    var site_name = '';

    if (handlerInput.requestEnvelope.request.intent.slots.name.hasOwnProperty('value')){
        if (handlerInput.requestEnvelope.request.intent.slots.name.resolutions.resolutionsPerAuthority[0].status.code === "ER_SUCCESS_MATCH"){
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.name.resolutions.resolutionsPerAuthority[0].values[0].value.name;
        }
        else{
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.name.value;
        }
    }
    
    query = 'list aps' + site_name;
    
    //sending post request 
    await post_request(options, query)
    .then((response) => {
        const rcvd_data = JSON.parse(response);
    
        for (let k = 0; k < rcvd_data.data.length; k++) {
            let item = rcvd_data.data[k];

            if (item.type ===  'text'){
                speakOutput = `${item.response[0]}. \n`;
            }

            else if (item.type === 'table'){
                item.response[0].item_list.forEach((li_item, i) => {
                    let name = li_item.Name;
                    
                    // if ((name.split(":").length) !== 6){   //ignoring device with mac as name
                    //     speakOutput = ` ${speakOutput}${li_item.Name}, `;
                    // }
                    if (i !== 0 && i === item.response[0].item_list.length - 1) {
                        // last record
                        speakOutput = `${speakOutput}and ${li_item.Name}.`;
                    } 
                    else{
                        // first and middle record
                        speakOutput = `${speakOutput}${li_item.Name}, `;
                    } 
                })                
                break;
            }
            else if (item.type === 'entityList'){
                let speech;

                item.response[0].list.forEach((item_2, i) => {
                    if (i !== 0 && i === item.response[0].list.length - 1) {
                        // last record
                        speakOutput = `${speakOutput}and ${item_2.title}.`;
                    } 
                    else{
                        // first and middle record
                        speakOutput = `${speakOutput}${item_2.title}, `;
                    } 
                })
                break;
            }
        }
    })
    .catch((err) => {
        console.log(`ERROR: ${err.message}`);
        speakOutput = `Some Error Occured: ${err.message}`
    });

    var cardTitle ;
    
    if (site_name === ''){
        cardTitle = 'Access Points in your Organization';
    }
    else{
        cardTitle = 'Access Points in ' + site_name.slice(4);
    }

    return handlerInput.responseBuilder
      .speak(speakOutput)
      .withSimpleCard(cardTitle, speakOutput)
      .reprompt('Do you need any other assistance?')
      .getResponse();
  },
};

const ListClientIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && Alexa.getIntentName(handlerInput.requestEnvelope) === 'ListClientIntent';
    },
    
    async handle(handlerInput) {
        
    let speakOutput = 'This is the default message';

    //options to create our post request to marvis
    const options = {
        method: 'POST',
        hostname: configs.api_host,
        path: '/api/v1/labs/orgs/' + configs.org_id + '/chatbot_converse',
        port: 443,
        headers: configs.headers
    }
    
    var site_name = '';
    
    if (handlerInput.requestEnvelope.request.intent.slots.var_name.hasOwnProperty('value')){
        if (handlerInput.requestEnvelope.request.intent.slots.var_name.resolutions.resolutionsPerAuthority[0].status.code === "ER_SUCCESS_MATCH"){
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.var_name.resolutions.resolutionsPerAuthority[0].values[0].value.name;
        }
        else{
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.var_name.value;
        }
    }
    
    query = 'list clients' + site_name;
    
    //sending post request 
    await post_request(options, query)
    .then((response) => {
        const rcvd_data = JSON.parse(response);
    
        for (let k = 0; k < rcvd_data.data.length; k++) {
            let item = rcvd_data.data[k];

            if (item.type ===  'text'){
                speakOutput = `${item.response[0]}.\n`;
            }

            else if (item.type === 'table'){
                item.response[0].item_list.forEach((li_item, i) => {
                    let name = li_item.Name;
                    // if ((name.split(":").length) !== 6){   //ignoring device with mac as name
                    //     speakOutput = ` ${speakOutput}${li_item.Name}, `;
                    // }
                    if (i !== 0 && i === item.response[0].item_list.length - 1) {
                        // last record
                        speakOutput = `${speakOutput}and ${li_item.Name}.`;
                    } 
                    else{
                        // first and middle record
                        speakOutput = `${speakOutput}${li_item.Name}, `;
                    } 
                })                
                break;
            }
            else if (item.type === 'entityList'){
                let speech;
                item.response[0].list.forEach((item_2, i) => {
                    if (i !== 0 && i === item.response[0].list.length - 1) {
                        // last record
                        speakOutput = `${speakOutput}and ${item_2.title}.`;
                    } 
                    else{
                        // first and middle record
                        speakOutput = `${speakOutput}${item_2.title}, `;
                    } 
                })
                break;
            }
        }
    })
    .catch((err) => {
        console.log(`ERROR: ${err.message}`);
        speakOutput = `Some Error Occured: ${err.message}`
    });

    var cardTitle;
    
    if (site_name === ''){
        cardTitle = 'Clients in your Organization';
    }
    else{
        cardTitle = 'Clients for ' + site_name.slice(4);
    }

    return handlerInput.responseBuilder
      .speak(speakOutput)
      .withSimpleCard(cardTitle, speakOutput)
      .reprompt('Do you need any other assistance?')
      .getResponse();
  },
};

const ListSwitchIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && Alexa.getIntentName(handlerInput.requestEnvelope) === 'ListSwitchIntent';
    },
    
    async handle(handlerInput) {
        
    let speakOutput = 'This is the default message';

    //options to create our post request to marvis
    const options = {
        method: 'POST',
        hostname: configs.api_host,
        path: '/api/v1/labs/orgs/' + configs.org_id + '/chatbot_converse',
        port: 443,
        headers: configs.headers
    }
    
    var site_name = '';
    
    if (handlerInput.requestEnvelope.request.intent.slots.site_name.hasOwnProperty('value')){
        if (handlerInput.requestEnvelope.request.intent.slots.site_name.resolutions.resolutionsPerAuthority[0].status.code === "ER_SUCCESS_MATCH"){
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.site_name.resolutions.resolutionsPerAuthority[0].values[0].value.name;
        }
        else{
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.site_name.value;
        }
    }
    
    query = 'list switch' + site_name;
    
    //sending post request 
    await post_request(options, query)
    .then((response) => {
        const rcvd_data = JSON.parse(response);
    
        for (let k = 0; k < rcvd_data.data.length; k++) {
            let item = rcvd_data.data[k];

            if (item.type ===  'text'){
                speakOutput = `${item.response[0]}.\n`;
            }

            else if (item.type === 'table'){
                item.response[0].item_list.forEach((li_item, i) => {
                    let name = li_item.Name;
                    
                    // if ((name.split(":").length) !== 6){   //ignoring device with mac as name
                    //     speakOutput = ` ${speakOutput}${li_item.Name}, `;
                    // }
                    if (i !== 0 && i === item.response[0].item_list.length - 1) {
                        // last record
                        speakOutput = `${speakOutput}and ${li_item.Name}.`;
                    } 
                    else{
                        // first and middle record
                        speakOutput = `${speakOutput}${li_item.Name}, `;
                    } 
                })                
                break;
            }
            else if (item.type === 'entityList'){
                let speech;

                item.response[0].list.forEach((item_2, i) => {
                    if (i !== 0 && i === item.response[0].list.length - 1) {
                        // last record
                        speakOutput = `${speakOutput}and ${item_2.title}.`;
                    } 
                    else{
                        // first and middle record
                        speakOutput = `${speakOutput}${item_2.title}, `;
                    } 
                })
                break;
            }
        }
    })
    .catch((err) => {
        console.log(`ERROR: ${err.message}`);
        speakOutput = `Some Error Occured: ${err.message}`
    });

    var cardTitle ;
    
    if (site_name === ''){
        cardTitle = 'Switches in your Organization';
    }
    else{
        cardTitle = 'Switches in ' + site_name.slice(4);
    }

    return handlerInput.responseBuilder
      .speak(speakOutput)
      .withSimpleCard(cardTitle, speakOutput)
      .reprompt('Do you need any other assistance?')
      .getResponse();
  },
};

const ListGatewayIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && Alexa.getIntentName(handlerInput.requestEnvelope) === 'ListGatewayIntent';
    },
    
    async handle(handlerInput) {
        
    let speakOutput = 'This is the default message';

    //options to create our post request to marvis
    const options = {
        method: 'POST',
        hostname: configs.api_host,
        path: '/api/v1/labs/orgs/' + configs.org_id + '/chatbot_converse',
        port: 443,
        headers: configs.headers
    }
    
    var site_name = '';
    
    if (handlerInput.requestEnvelope.request.intent.slots.site_name.hasOwnProperty('value')){
        if (handlerInput.requestEnvelope.request.intent.slots.site_name.resolutions.resolutionsPerAuthority[0].status.code === "ER_SUCCESS_MATCH"){
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.site_name.resolutions.resolutionsPerAuthority[0].values[0].value.name;
        }
        else{
            site_name = ' in ' + handlerInput.requestEnvelope.request.intent.slots.site_name.value;
        }
    }
    
    query = 'list gateways' + site_name;
    
    //sending post request 
    await post_request(options, query)
    .then((response) => {
        const rcvd_data = JSON.parse(response);
    
        for (let k = 0; k < rcvd_data.data.length; k++) {
            let item = rcvd_data.data[k];

            if (item.type ===  'text'){
                speakOutput = `${item.response[0]}.\n`;
            }

            else if (item.type === 'table'){
                item.response[0].item_list.forEach((li_item, i) => {
                    let name = li_item.Name;
                    
                    // if ((name.split(":").length) !== 6){   //ignoring device with mac as name
                    //     speakOutput = ` ${speakOutput}${li_item.Name}, `;
                    // }
                    if (i !== 0 && i === item.response[0].item_list.length - 1) {
                        // last record
                        speakOutput = `${speakOutput}and ${li_item.Name}.`;
                    } 
                    else{
                        // first and middle record
                        speakOutput = `${speakOutput}${li_item.Name}, `;
                    } 
                })                
                break;
            }
            else if (item.type === 'entityList'){
                let speech;

                item.response[0].list.forEach((item_2, i) => {
                    if (i !== 0 && i === item.response[0].list.length - 1) {
                        // last record
                        speakOutput = `${speakOutput}and ${item_2.title}.`;
                    } 
                    else{
                        // first and middle record
                        speakOutput = `${speakOutput}${item_2.title}, `;
                    } 
                })
                break;
            }
        }
    })
    .catch((err) => {
        console.log(`ERROR: ${err.message}`);
        speakOutput = `Some Error Occured: ${err.message}`
    });
    
    var cardTitle ;
    
    if (site_name === ''){
        cardTitle = 'Gateways in your Organization';
    }
    else{
        cardTitle = 'Gateways in ' + site_name.slice(4);
    }

    return handlerInput.responseBuilder
      .speak(speakOutput)
      .withSimpleCard(cardTitle, speakOutput)
      .reprompt('Do you need any other assistance?')
      .getResponse();
  },
};

//To handle Troubleshoot devices
const TroubleshootIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && Alexa.getIntentName(handlerInput.requestEnvelope) === 'TroubleshootIntent';
    },
    
    async handle(handlerInput) {
        
    let speakOutput = 'This is the default message';
    let device_name = '';
    
    if (handlerInput.requestEnvelope.request.intent.slots.device_name.hasOwnProperty('value')){
        if (handlerInput.requestEnvelope.request.intent.slots.device_name.resolutions.resolutionsPerAuthority[0].status.code === "ER_SUCCESS_MATCH"){
            device_name = handlerInput.requestEnvelope.request.intent.slots.device_name.resolutions.resolutionsPerAuthority[0].values[0].value.name;
        }
        else{
            device_name = handlerInput.requestEnvelope.request.intent.slots.device_name.value;
        }
    }

    //options to create our post request to marvis
    const options = {
        method: 'POST',
        hostname: configs.api_host,
        path: '/api/v1/labs/orgs/' + configs.org_id + '/chatbot_converse',
        port: 443,
        headers: configs.headers
    }
    
    //hardcoding device name for now because device names are very wierd like: Jon_10, Abhi's Home etc. and ALexa obvio is not able to interpret them correctly.
    //leaving device name as "Home" just for testing a different type of input slot.
    //Hard coding different values for testing
    
    query = 'Troubleshoot ' + device_name;
    
    //sending post request 
    await post_request(options, query)
    .then((response) => {
        const rcvd_data = JSON.parse(response);
        
        // TO HANDLE DIFFERENT TYPES OF RESPONSES
        // Later, will create a seperate function to iterate through whole response and display out only necessary details

        for (let k = 0; k < rcvd_data.data.length; k++) {
            let item = rcvd_data.data[k];
            
            if (item.type ===  'text'){
                
                if (item.response[0] === 'Can you tell me something about the device (mac/name)?' || item.response[0] === 'Can you tell me the the site name?'){
                    speakOutput = 'Unable to find your device or site. Please try again.';
                }
                else{
                    speakOutput = `${item.response[0]}`;
                }
            }

            else if (item.type === 'options'){
                let speech;

                speakOutput = `${speakOutput}`;

                if (item.response.length === 1){
                    speech = `\n${item.response[0].title}. ${item.response[0].description}`;
                    speakOutput = `${speakOutput + speech} Here are some details. `;

                    item.response[0].response.forEach((i_item, idx) => {
                        if (i_item.type === 'text' && i_item.response[0] !== 'Additional information listed below:'){
                            speech = `\n${(idx+1)}. ${i_item.response[0]}`;
                            speakOutput = `${speakOutput + speech}`;
                        }
                    })
                }
                else{
                    item.response.forEach((item_2, idx) => {
                        speech = `\n${(idx+1)}. ${item_2.title}. ${item_2.description}`;
                        speakOutput = `${speakOutput + speech}`;
                    })
                }
                break;
            }

            else if (item.type === 'entityList'){
                speakOutput = `${speakOutput} Found multiple devices with this name. Please be more specific about which device you want to troubleshoot. For example, say: `;
                speakOutput = `${speakOutput} ${item.response[0].list[0].display.phrase}`
                break;
            }
        }
    })
    .catch((err) => {
        console.log(`ERROR: ${err.message}`);
        speakOutput = `Some Error Occured: ${err.message}`
    });
    
    var cardTitle = "Troubleshooting " + device_name;
    
    return handlerInput.responseBuilder
      .speak(speakOutput)
      .withSimpleCard(cardTitle, speakOutput)
      .reprompt('Do you need any other assistance?')
      .getResponse();
  },
};


//Responding to user saying Yes.

const YesIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && Alexa.getIntentName(handlerInput.requestEnvelope) === 'YesIntent';
    },
    handle(handlerInput) {
        const speakOutput = 'Go on. I\'m listening.';

        return handlerInput.responseBuilder
            .speak(speakOutput)
            .reprompt()
            .getResponse();
    }
};

function getUserName(){
    
    
}

const HelpIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && Alexa.getIntentName(handlerInput.requestEnvelope) === 'AMAZON.HelpIntent';
    },
    handle(handlerInput) {
        const speakOutput = 'Hey! This is your navigation guide.';

        return handlerInput.responseBuilder
            .speak(speakOutput)
            .reprompt(speakOutput)
            .getResponse();
    }
};

const CancelAndStopIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && (Alexa.getIntentName(handlerInput.requestEnvelope) === 'AMAZON.CancelIntent'
                || Alexa.getIntentName(handlerInput.requestEnvelope) === 'AMAZON.StopIntent');
    },
    handle(handlerInput) {
        const speakOutput = 'Okay!';

        return handlerInput.responseBuilder
            .speak(speakOutput)
            .getResponse();
    }
};

/* *
 * SessionEndedRequest notifies that a session was ended. This handler will be triggered when a currently open 
 * session is closed for one of the following reasons: 1) The user says "exit" or "quit". 2) The user does not 
 * respond or says something that does not match an intent defined in your voice model. 3) An error occurs 
 * */
const SessionEndedRequestHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'SessionEndedRequest';
    },
    handle(handlerInput) {
        console.log(`~~~~ Session ended: ${JSON.stringify(handlerInput.requestEnvelope)}`);
        // Any cleanup logic goes here.
        return handlerInput.responseBuilder.getResponse(); // notice we send an empty response
    }
};
/* *
 * The intent reflector is used for interaction model testing and debugging.
 * It will simply repeat the intent the user said. You can create custom handlers for your intents 
 * by defining them above, then also adding them to the request handler chain below 
 * */
const IntentReflectorHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest';
    },
    handle(handlerInput) {
        const intentName = Alexa.getIntentName(handlerInput.requestEnvelope);
        const speakOutput = `You just triggered ${intentName}`;

        return handlerInput.responseBuilder
            .speak(speakOutput)
            //.reprompt('add a reprompt if you want to keep the session open for the user to respond')
            .getResponse();
    }
};
const FallbackIntentHandler = {
    canHandle(handlerInput) {
        return Alexa.getRequestType(handlerInput.requestEnvelope) === 'IntentRequest'
            && Alexa.getIntentName(handlerInput.requestEnvelope) === 'AMAZON.FallbackIntent';
    },
    handle(handlerInput) {
        const speakOutput = 'I didn\'t understood your query. Please try again.';

        return handlerInput.responseBuilder
            .speak(speakOutput)
            .reprompt('Do you need any other assistance?')
            .getResponse();
    }
};
/**
 * Generic error handling to capture any syntax or routing errors. If you receive an error
 * stating the request handler chain is not found, you have not implemented a handler for
 * the intent being invoked or included it in the skill builder below 
 * */
const ErrorHandler = {
    canHandle() {
        return true;
    },
    handle(handlerInput, error) {
        const speakOutput = 'Sorry, I had trouble doing what you asked. Please try again.';
        console.log(`~~~~ Error handled: ${JSON.stringify(error)}`);

        return handlerInput.responseBuilder
            .speak(speakOutput)
            .reprompt('Do you need any other assistance?')
            .getResponse();
    }
};

/**
 * This handler acts as the entry point for your skill, routing all request and response
 * payloads to the handlers above. Make sure any new handlers or interceptors you've
 * defined are included below. The order matters - they're processed top to bottom 
 * */
exports.handler = Alexa.SkillBuilders.custom()
    .addRequestHandlers(
        LaunchRequestHandler,
        QueryIntentHandler,
        UnhappyUsersIntentHandler,
        UnhappySwitchIntentHandler,
        UnhappyGatewayIntentHandler,
        UnhappyApsIntentHandler,
        ListApsIntentHandler,
        ListClientIntentHandler,
        ListSwitchIntentHandler,
        ListGatewayIntentHandler,
        TroubleshootIntentHandler,
        YesIntentHandler,
        HelpIntentHandler,
        CancelAndStopIntentHandler,
        FallbackIntentHandler,
        SessionEndedRequestHandler,
        IntentReflectorHandler)
    .addErrorHandlers(
        ErrorHandler)
    .withCustomUserAgent('sample/hello-world/v1.2')
    .lambda();