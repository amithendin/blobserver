# Simple data storage server
A simple file server to which you can upload and download files using http
requests.
The server stores the files together in big blobs, and separates blobs when the blob
file hits the defined size limit.
Deleted files will be wiped from the blob on a defined interval.
Uses sqlite to keep track of files and their location in the blob.

**Warning** not yet thoroughly tested, use with caution.

## API
To fetch data block by id
```agsl
curl -XGET "http://<your server address and port>/<block id>"
```
response will be the file content stream with the mime type which was detected during
upload

To upload data block
```agsl
curl -d @path/to/data.json "http://<your server address and port>/"
```
or
```agsl
curl -XPOST -d 'data to post' "http://<your server address and port>/"
```
response will be a json object `{"block_id": <generated block id for the uploaded data>}`

To delete data block by id
```agsl
curl -XDELETE "http://<your server address and port>/<block id>"
```
This will mark the block for deletion and next cleaning cycle the block will
be deleted from it's respective blob file.

response will be a json object `{"msg": "deleted"}`

In any case, if there is an error during the process, the server will 
respond with a json object `{"err": <the error message>}` and with an appropraite
http response code

## Config

Apon first running the server, it will generate a file named config.json if one does
not exist in the same directory as the server executable. You can alter this json
file to change the configuration of the server and your changes will not be overwritten
keep in mind that in order for config changes to take affect you will need to restart 
the server.

Example config file
```agsl
{
  "data_dir": "./data",
  "max_blob_size_bytes": 5120,
  "clean_interval_secs": 60,
  "port": 3000,
  "CORS": [
    {
      "key": "Access-Control-Allow-Origin",
      "val": "*"
    },
    {
      "key": "Access-Control-Allow-Headers",
      "val": "*"
    },
    {
      "key": "Access-Control-Allow-Methods",
      "val": "POST, GET, OPTIONS, DELETE"
    }
  ]
}
```
I think the config variable names are pretty self explanatory.
