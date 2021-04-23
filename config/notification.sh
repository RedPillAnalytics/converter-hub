# Create the PubSub notification for cloud storage
gsutil notification create -t sessions-upload -f json gs://rpa-converter
gsutil notification list gs://rpa-converter
gsutil versioning set on gs://rpa-converter