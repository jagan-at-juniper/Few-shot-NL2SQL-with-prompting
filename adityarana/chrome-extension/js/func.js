var org_id, token;
var hostname = "https://api.mistsys.com/";

async function get_token_orgID(){
    await chrome.storage.local.get({"org_id": null, "token": null}, function(items){
        console.log(items);

        org_id = items['org_id'];
        token = items['token'];    
        console.log("1")   
        $("#contentBody").append("<div class='outerMessageBody in' id='loading'><div class='incomingMessage'>&#129300; &#129300; &#129300</div></div> ");
        scrollToBottom();
        postData = getPostData("init_chat")
        console.log("2")
        make_api_call(hostname, postData, org_id, token);
    });
}

function scrollToBottom() {
    $('#contentBody').scrollTop($('#contentBody')[0].scrollHeight);
}

function removeLoad() {
    $("#loading").remove(); 
}

function addResponseText(text, class_name="") {
    $("#contentBody").append("<div class='outerMessageBody in " + class_name + "'><div class='incomingMessage'>" + text + "</div></div>");
    
}

function invalidCredsHandler(message) {
    removeLoad();
                addResponseText(message)
                
                setTimeout(function () {
                    document.location.href = 'signin.html';
                    chrome.storage.local.set({"token": null});
                }, 4000);
}

function  getPostData(query) {
    var data = JSON.stringify({
        "type": "phrase",
        "phrase": query,
        "attempt": "first",
        "user_metadata": {
          "time_zone": "America/Los_Angeles",
          "first_name": "Chrome-ext User",
          "user_id": "",
          "org_id": org_id
        }
    });
    return data
}

function make_api_call(hostname, data, org_id, token) {
    var xhr = new XMLHttpRequest();
    xhr.withCredentials = true;

    xhr.addEventListener("readystatechange", function() {
        if(this.readyState === 4) {
            if (this.status == 401){
                console.log('Unauthorized');
                message = "<strong>Invalid Token.</strong> Signing out to home page..."
                invalidCredsHandler(message)
            }
            else if (this.status == 404){
                console.log('404');
                message = "<strong>Invalid Org ID.</strong> Signing out to home page... > "
                invalidCredsHandler(message)
            }
        
            else{
                res = JSON.parse(this.responseText);
                console.log(res.data);
            
                removeLoad();
                parsingResponse(res.data);
                scrollToBottom();
            }
        }
    });
    var api_url = hostname + "api/v1/labs/orgs/" + org_id + "/chatbot_converse";

    xhr.open("POST", api_url);
    xhr.setRequestHeader("Access-Control-Allow-Origin", "*");
    xhr.setRequestHeader("Content-Type", "application/json");
    xhr.setRequestHeader("Authorization", "Token " + token);

    xhr.send(data);

}

function parsingResponse(response) {
    
    for (var i = 0; i < response.length; i++){
        if (response[i].type === 'text'){
            if (i > 0){
                break
            }
            textResponseHandler(response[i])
        }
        else if (response[i].type === 'table'){
            className = "noTopPad noTopMargin"
            tableResponseHandler(response[i], className)
        }
        else if (response[i].type === 'options'){
            className = "noTopMargin"
            optionResponseHandler(response[i], className)
            
        }
        else if (response[i].type === 'entityList'){
            className = "noTopMargin noTopPad"
            entityResponseHandler(response[i], className)
        }
    }
}

function textResponseHandler(response, className = "") {
    resData = "";
    response.response.forEach(element => {
        resData += element;
        resData += " ";
    });
    if (resData.includes("some options to get started")){
        resData = resData.split(".", 1)
    }

    addResponseText(resData, className);
    
}
function tableResponseHandler(response, className = "") {
    var j;
    htmlElement = `
        <table>
            <tr>
                <th>Name</th>
                <th>Mac</th>
            </tr>`

    for (j = 0; j < response.response[0].item_list.length && j < 5; j ++){
        element = response.response[0].item_list[j]
        htmlElement += '<tr><td>' + element.Name + '</td><td>' + element.Mac + '</td></tr>';
    }
    htmlElement += '</table>'
    addResponseText(htmlElement, className);
}
function optionResponseHandler(response, className = "") {
    htmlElement = ""
    response.response.forEach(element => {
        htmlElement += "<div class = 'optContainer'><div class = 'optionTitle'>" 
                    + element.title 
                    + "</div><div class = 'optionDesc'>" 
                    + element.description 
                    + '</div></div>'
    })
    addResponseText(htmlElement, className); 
    
}
function  entityResponseHandler(response, className = "") {
    htmlElement = ""
    response.response[0].list.forEach(element => {
        // htmlElement += "<div class = 'icon'><img src='../images/usr.png' class = 'respIcon'></img></div><div class = 'entContainer inlineBlock'><div class = 'optionTitle'>" 
        //             + element.title 
        //             + "</div><div class = 'optionDesc'>" 
        //             + element.description 
        //             + '</div></div>'
        htmlElement += "<div class = 'entContainer inlineBlock'><div class = 'optionTitle'>" 
                    + element.title 
                    + "</div><div class = 'optionDesc'>" 
                    + element.description 
                    + '</div></div>'
    })
    addResponseText(htmlElement, className); 
}

$(function (){
    get_token_orgID()

    $("#signOut").on('click', function () {
        chrome.storage.local.set({"org_id": null, "token": null});
        document.location.href = 'signin.html';
    })

    $("#sendMessage").on('click', function (){
        var query = $("#queryMessage").val();
        query = query.trim();

        if (query.length > 0){
            $("#queryMessage").val('');
            $("#contentBody").append("<div class='outerMessageBody out'><div class='outgoingMessage'>" + query + "</div></div> ");
            $("#contentBody").append("<div class='outerMessageBody in' id='loading'><div class='incomingMessage'>&#129300; &#129300; &#129300</div></div> ");
            scrollToBottom();
            postData = getPostData(query)
            
            make_api_call(hostname, postData, org_id, token);
        }
    })

    $("#queryMessage").on('keypress', function(key) {
        if(key.which == 13) {
            var query = $("#queryMessage").val();
            query = query.trim();

            if (query.length > 0){
                $("#queryMessage").val('');
                $("#contentBody").append("<div class='outerMessageBody out'><div class='outgoingMessage'>" + query + "</div></div> ");
                $("#contentBody").append("<div class='outerMessageBody in' id='loading'><div class='incomingMessage'>&#129300; &#129300; &#129300</div></div> ");
                scrollToBottom();
                postData = getPostData(query)
                
                make_api_call(hostname, postData, org_id, token);
            }
        }
    });

    $(window).on('keypress', function(key) {
        if(key.which == 13) {
            $("#queryMessage").trigger('focus');
        }
    });

    $("#closeIcon").click(function (){
        window.close();
    })
})

