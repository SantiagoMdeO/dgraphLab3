# iteso-bdnr-dgraph

A place to share dgraph app code

### Setup a python virtual env with python dgraph installed
```
# If pip is not present in you system
sudo apt update
sudo apt install python3-pip

# Install and activate virtual env (Linux/MacOS)
python3 -m pip install virtualenv
python3 -m venv ./venv
source ./venv/bin/activate

# Install and activate virtual env (Windows)
python3 -m pip install virtualenv
python3 -m venv ./venv
.\venv\Scripts\Activate.ps1

# Install project python requirements
pip install -r requirements.txt
```

### To load data
Ensure you have a running dgraph instance
i.e.:
```
docker run --name dgraph -d -p 8080:8080 -p 9080:9080  dgraph/standalone
```
Run main.py
i.e.:
```
python3 main.py
```
