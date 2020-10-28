module github.com/augustwenty/go-crash/transforms

go 1.15

require (
	github.com/augustwenty/go-crash/messages v0.0.0-00010101000000-000000000000
	github.com/segmentio/kafka-go v0.4.8
)

replace github.com/augustwenty/go-crash/messages => ../messages
