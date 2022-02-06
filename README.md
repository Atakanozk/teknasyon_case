## Hi! Welcome to taxi driver case solution
This repository created for teknasyon taxi driver case.

Repository originnaly designed to work with gcp. In addition to that to see full outcome of etl flow, linux vm or cloud composer needed!

### **Methods**
This porject based on GCP that's why we need to open a new gcp account. But don't worry google gives 300$ credit. 
After opening the account follow these steps:

* Open Bigquery and create dataset with name **taxi_ds** for your project. In the code "teknasyon-340116.taxi_ds". Project and dataset should be updated for new project id.
* teknasyon-340116 project name should be changed to your_project_id in all scripts in the later part
* Create a service account. In the IAM & Admin Pannel click service accounts. On the upper left side of the screen click create service account. Give it a name and id. 
* After service account created from IAM Pannel give your service account Bigquery Admin and Storage Admin permission.
* On the Service accounts panel click three dots on the right side of the account and click manage keys. Add a new key for your account and download it as **json** to your machine.
* Click cloud storage panel.
* Create a new bucket as **taxib** and a **folder** named first_week.

 If you did all of these steps you are g2g for cloning!
 
* Clone repo to your computer. 
* Create a .ini file named as config.ini
* Move the service account json folder to the main folder of project.
* Open config.ini file and add ``` [gcp]
                                    cred_file = your_service_account_json_file_name.json  ```
* Change all teknasyon-340116 to your project_id from scripts (python)

From now on you project installation finished.

* Install python and pip. 
* Move to project wd.
* Install virtualenv ``` pip install virtualenv ``` and open a new virtualenv named as venv with ```python -m venv venv ```
* Activate ve and install requirements with the following statement ``` pip install -r requirements.txt ``` Note : If you have any error with airflow while downloading requirements just pass.

You can run the codes manually now !
