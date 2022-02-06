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

From now on you need to open compute engine in order to automate the system and make a vm as I do. But you don't need to do that part. If you already have a machine with airflow installed then just change the bash scripts from dags/teknasyon_dag.py to your machine pwd. 

### Compute engine part (If you already have airflow on your machine, pass this part)
* Enable compute engine api.
* Create a vm. I suggest Ubuntu. 10 gb storage is enough for this project. Small machine will be enough. 
* Dont forget to click allow http and https methods.
When vm is created follow these steps:
``` sudo apt install python ```
``` apt-get install software-properties-common ```
``` apt-get install python-pip ```
* Create ve and activate it. 
* Then install airflow ``` pip install apache-airflow ```
* init db as ``` airflow db init ``` (sqlite is enough for our project)
* Then create a firewall.
* On the gcp panel **firewall** section, create firewall rule give it a name and priortiy as 8080.
* Spesificed target will be all instances. 
* Enable tcp and create rule.
* On machine Create a superuser ```airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin```
* Now you can run ``` airflow webserver -p 8080 ``` and on the other session ``` airflow scheduler ``` 
* To see you web page open your compute engine panel and copy vm external ip. For example 34.139.59.188 and paste it as 34.139.59.188:8080 
* On the login page enter admin - admin your username and password and now you can see your airflow.
* On the vm side chane **airflow.cfg** with ```vim airflow.cfg``` and change **dags_folder = /home/username/teknasyon_case/dags**
You can see teknasyon_dag on airflow now and If you did all of these step run the dag.
You will see tables on bigquery under your_project_id.your_data_set.tables.
You can also see partitioned data in the cloud storage bucket called taxib. Data partitioned by Y - M - D as csv.
For my own methods and screenshots I will share doc with you.

Thank you!
Atakan Özkan


