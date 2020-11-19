# go-crash

Don't crash the boats.


# Running the Demo

## Prerequisites:
Install Docker
```
brew cask install docker
```
Install Parcel Bundler:
```
npm install -g parcel-bundler
```

## Running Kafka:
Clone the repository:
```
git clone git@github.com:wurstmeister/kafka-docker.git
```

In `docker-compose-single-broker.yaml` change the value of `KAFKA_ADVERTISTED_HOST_NAME` on line 12 to `localhost`, like this:
```
KAFKA_ADVERTISED_HOST_NAME: localhost
```

Start Kafka-docker:
```
docker-compose -f docker-compose-single-broker.yml up -d
```

## Start Boat Data generator:
You need to run this first to get the topic set up:

```
go run cmd/boatDataGenerator/main.go
```

Once you've run the data generator once, you can use ctrl-c to stop it for now.

## Start the Speedboat and Sailboat processors:
You need to perform this part twice in two different terminal windows. When you run `sbt run` you'll be prompted to choose one or the other to start. Run them both in separate windows.
```
cd scala/scala-crash
sbt run
```

## Start kafka2websocket
```
cd dashboard/kafka2websocket
./k2ws
```

After running this command, you should be able to hit the k2ws test page at http://localhost:8080/test. Click "Open" on the top bar to open the websocket connection. If you run the boat data generator again, you should see messages coming in on this window.

## Start Web GUI
```
cd dashboard/frontend
parcel index.html
```

You should now be able to hit `http://localhost:1234` and see the Boat GUI.

## Start Boat Data generator:
Now you can run the boat data generator, and watch the boats move on the map.
```
go run cmd/boatDataGenerator/main.go
```