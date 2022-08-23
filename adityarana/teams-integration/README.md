# Marvis Integration with Teams
## Source code contains Web-app to process ms-teams bot request

### How to setup the Microsoft Teams bot for your organization

### <u>Step 1:</u> Setup a Teams bot using MS Developer Portal
1. In your MS Teams, go to `MS Teams App Store` and search and install `Developer Portal`. This allows you to setup a MS Teams bot.
2. Open `Developer Portal` and re-direct to `Apps` tab. Click on create a `New App` and give the bot a name, **Marvis**.
3. This will create an app called `Marvis`. Go to `Configure` > `Basic Information` tab on the left side pane and Edit all the required informations like: **Descriptions**, **Developer Information**, **Privacy Policy** etc. Leave  `Application(Client) ID` empty for now and save.
4. Now, go to `App Features` tab and under **Select a feature to add**, choose `Bot`. Under **Identify your bot** choose `Create a new bot`.
5. You will be redirected to **Bot Management** page. There, click on `New Bot` and give it a name, `Marvis`, and hit **Add**.
6. After successful creation. The bot endpoint address you see here will have to point towards your backend web application. We'll come back to this later.
7. Open [Azure Portal](https://portal.azure.com), login with your Azure ID, and redirect to `App Registration` section. You will see a new application created with name **Marvis**. Open the application and copy `Application(client) ID` and paste it in the **Application(Client) ID** section which was left blank earlier.
8. Again, redirect to `Configure > App Features`, select `Bot`, but this time, choose `Enter a Bot ID` and paste the same App ID that you copied. Choose all scopes, i.e., **Personal**, and **Teams** and click on Save.


### <u>Step 2:</u> Generating your Microsoft App ID & Password
1. The Microsoft App ID would be the `Application(client) ID` you copied from Azure Portal.
2. In your Azure Portal, redirect to `App Registrations` section and open your bot application. Go to `Certificates and Secrets` tab on the left pane and click on create `New Client Secret`.
3. Give it a description and expiration periods. Note: You need to regenrate the password once it expires. Click on Create.
4. The Password value is stored in the tab `Value`. Copy it and save it somewhere. You won't be able to view it later.


### <u>Step 3:</u> Hosting your Web Application
1. You need to host the web application using any one of the cloud providers like AWS, GCP etc.
2. Next, you need to setup the environment variables for the server where your application is being hosted. Every cloud providers have their own way of setting the environment variables. These will contains all the sensitive information of the Teams bot like Application ID, Secret key, Mist Token, Org ID etc.
3. All the environment variables you need to create are given in **.sample_env** file. These are:<br>
    **MicrosoftAppId**=Application(client) ID<br>
    **MicrosoftAppPassword**=App Pw you created in Step 2<br>
    **MIST_CHANNEL_TOKEN**=<br>
    **MIST_CHANNEL_ORG_ID**=<br>
    **MongoDB_pw**=<br>


### <u>Step 4:</u> Pointing your Marvis Teams bot to your hosted Web Application
1. Log on to [Botframework Portal](https://dev.botframework.com). Go to `My Bots` and open your bot application.
2. Redirect to `Settings` tab and under `Configuration` > `Messaging Endpoint`, point to **/api/messages** endpoint of your web app url. Ex: if your web app url is **https://mycustombot.com** then, you need to point to **https://mycustombot.com/api/messages**
