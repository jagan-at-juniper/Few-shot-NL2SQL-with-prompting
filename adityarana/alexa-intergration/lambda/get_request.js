const client = require("https");
const configs = require(__dirname + "/config.js")

module.exports = (opt) => new Promise((resolve, reject) => {
  
    const request = client.get(opt, (response) => {
    
        if (response.statusCode < 200 || response.statusCode > 299) {
            console.log(response.statusCode)
            reject(new Error(`Failed with status code: ${response.statusCode}`));
        }
        const body = [];
        response.on('data', (chunk) => body.push(chunk));
        response.on('end', () => resolve(body.join('')));
    });
    request.on('error', (err) => reject(err));
});