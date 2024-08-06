# Readme

`kafka.py` contains an example of connecting to the Kafka provided by Ditto and inserting
changes into a MongoDB cluster.
It's designed to show a basic integration of how to handle the change stream, but in practice
needs a lot more like handling of upserts and deletes.

## Setup
### Certificates
First go to the Ditto portal and find your App.
On the "Live Query Settings" page you will see a "Kafka Integration" section.
Download both the cluster certificate and the user certificate.

You need to convert these into PEM format to be used by the Confluent SDK, the steps to do so using
`openssl` are as follows.

Generate cluster cert:

```bash
openssl pkcs12 -in cluster.p12 -out cluster.cert -nokeys -chain
# This will prompt you for the cert password, use the cluster password in the Ditto Portal
```

Generate user cert:

```bash
openssl pkcs12 -in user.p12 -out user.cert -nokeys -chain
# This will prompt you for the cert password, use the user password in the Ditto Portal
```

Generate user key:

```bash
openssl pkcs12 -in user.p12 -out ~/Documents/kafka/user.key -nocerts -nodes
# This will prompt you for the cert password, use the user password in the Ditto Portal
```

### Pre-requisites
This requires Python 3.12+, the requirements can be installed using:

```bash
pip install -r requirements.txt
```

Once installed and with the certs/keys moved to the script's working directory, the script can be 
invoked with a command like the following (credentials redacted):

```bash 
python kafka.py 'mongodb+srv://<user>:<password>@<cluster_url>.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0' '<kafka_topic>'
```

You can get your MongoDB URL (first argument) from Atlas or elsewhere, the kafka topic should be copied
and pasted from the Ditto Portal.