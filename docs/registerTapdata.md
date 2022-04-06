## PDK Registration
```
./bin/tap register
  Push PDK jar file into Tapdata
      [FILE...]            One ore more pdk jar files
  -a, --auth=<authToken>   Provide auth token to register
  -h, --help               TapData cli help
  -l, --latest             whether replace the latest version, default is true
  -t, --tm=<tmUrl>         Tapdata TM url
  
./bin/tap register -a token-abcdefg-hijklmnop -t http://host:port dist/your-connector-v1.0-SNAPSHOT.jar
```
