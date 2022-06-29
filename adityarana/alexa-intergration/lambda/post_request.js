const client = require("https");
const configs = require(__dirname + "/config.js")

module.exports = (opt, query) => new Promise((resolve, reject) => {

    var request = client.request(opt, (response) => {
        var chunks = [];
        if (response.statusCode < 200 || response.statusCode > 299) {
            console.log(response.statusCode)
            reject(new Error(`Failed with status code: ${response.statusCode}`));
        }
        const body = [];
      
        response.on("data", (chunk) => body.push(chunk));
      
        response.on('end', () => resolve(body.join('')));
  
        response.on("error", (error) => reject(error));
    });
  
    var postData = JSON.stringify({
        "type": "phrase",
        "phrase": query,
        "attempt": "first",
        "user_metadata": configs.user_metadata
    });
  
    request.write(postData);
  
    request.end();
});

