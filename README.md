# ams-alert-monitoring documentation


## Description
This repository contains the full code for the ams-alert-monitoring running on an EC2 instance in the AWS-CustomerSuccessTraining account

## Accessing the server
The AWS-CustomerSuccessTraining account can be accessed from https://desktop.pingone.com by selecting "Amazon Web Services" and then selecting the "EC2" service.

The instance name is `AMS_Alert_Monitoring_Emanuele_B` and the best way to connect to it is open the SSh configuration file in visual studio code and paste the following:

```
Host AMS_Alert_Monitoring_Emanuele_B
  HostName ec2-52-59-254-219.eu-central-1.compute.amazonaws.com
  User ubuntu
  IdentityFile path/to/your/ams-alert-monitoring/auth/EmanueleBonura-AMS-test1.pem
  ```

  The .pem file has to be sent separately as it's not included in the repo.

## Turning on the alert
Once you are connected to the instance, navigate to the ams-alert-monitoring folder and open a tmux session by typing:
```tmux```

Warning! If the tool is already running and you want to enter an existing session, with: ```tmux attach```

Once inside the tmux session you can enter the python enviroment with `pipenv shell` and run the main alerting loop with `python3 main-2.py`, you can now close the ssh connection, as the tool will keep running in the EC2 instance.

## Structure of the tool

Under utilities, you will find the clients responsible for connecting to each service used by the tool:
```
└── utilities
    ├── athena_client.py
    ├── google_client.py
    ├── helper.py
    └── slack_client.py
```

Both Athena and Slack clients leverage credentials contained in the `~/.dpcfg.ini` file, inside the server root folder, while google automatically generates a `auth/token.pickle` file.

The spreadsheet used to configure the alerts can be found here: https://docs.google.com/spreadsheets/d/10QCryRqYBlS-kE_ExHaGaSZU2JfS88BGcBss-KYr0Wk


## Slack
 - app name: `Connector_attack_monitoring`
 - channel name: `#connector_attack_alerting`

Slack app can be configured from https://api.slack.com/apps and clicking on `Connector_attack_monitoring`, no configuration change should be required
