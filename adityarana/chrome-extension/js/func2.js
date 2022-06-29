$(function (){
    chrome.storage.local.get({"org_id": null, "token": null}, function(items){
        console.log(items);

        org_id = items['org_id'];
        token = items['token'];
        if (org_id && token){
            document.location.href = 'index.html';
        }
    });

    $("#loginBtn").on('click', function () {
        show_status();

        org_id = $('#org').val();
        token = $('#token').val();

        if (org_id && token){
            $('.id').css('display', 'none');
            $('.token').css('display', 'none');

            test_creds();
            
        }
        else{
            if (org_id){
                $('.id').css('display', 'none');
            }

            else{
                $('.id').css('display', 'inline-block');
            }

            if (token){
                $('.token').css('display', 'none');
            }
            else{
                $('.token').css('display', 'inline-block');
            }
        }
    })

    function test_creds() {
        var data = JSON.stringify({
            "type": "phrase",
            "phrase": "init_chat",
            "attempt": "first",
            "user_metadata": {
              "time_zone": "America/Los_Angeles",
              "first_name": "Chrome-ext User",
              "user_id": "",
              "org_id": org_id
            }
          });
          
        var xhr = new XMLHttpRequest();
        xhr.withCredentials = true;
        
        xhr.addEventListener("readystatechange", function() {
            if(this.readyState === 4) {
                
                if (this.status == 401){
                    console.log('Unauthorized');
                    modify_status(this.status);
                }
                else if (this.status == 404){
                    console.log('404');
                    modify_status(this.status);
                }
                else{
                    res = JSON.parse(this.responseText);
                    console.log(res);

                    set_variables(org_id, token);
                    document.location.href = 'index.html';
                }
              
            }
        });

        var api_url = "https://api.mistsys.com/api/v1/labs/orgs/" + org_id + "/chatbot_converse";
        console.log(api_url);
        
        xhr.open("POST", api_url);
        xhr.setRequestHeader("Access-Control-Allow-Origin", "*");
        xhr.setRequestHeader("Content-Type", "application/json");
        xhr.setRequestHeader("Authorization", "Token " + token);
        
        xhr.send(data);
        
    }

    function set_variables(org_id, token) {
        chrome.storage.local.set({ "org_id": org_id, "token": token});
    }

    function show_status(){
        $('#validation').text('Validating...');
        $('#validation').css('color', '#1c9621');
        $('#validation').css('display', 'inline-block');
    }

    function modify_status(err) {
        if (err == 401){
            $('#validation').text('Invalid Token...');
            $('#validation').css('color', '#8B0000');
        }

        else if (err == 404){
            $('#validation').text('Invalid Org ID...');
            $('#validation').css('color', '#8B0000');
        }
    }
})
