module github.com/datazip-inc/olake

go 1.24.0

require github.com/segmentio/kafka-go v0.4.49

require github.com/stretchr/testify v1.11.1 // indirect

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	golang.org/x/net v0.42.0 // indirect
)

replace (
	cloud.google.com/go/compute => cloud.google.com/go/compute v1.23.3
	cloud.google.com/go/compute/metadata => cloud.google.com/go/compute/metadata v0.6.0
	google.golang.org/genproto => google.golang.org/genproto v0.0.0-20240123012728-ef4313101c80
)
