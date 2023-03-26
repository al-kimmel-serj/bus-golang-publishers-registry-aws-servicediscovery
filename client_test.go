package bus_golang_publishers_registry_aws_servicediscovery_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/al-kimmel-serj/bus"
	registry "github.com/al-kimmel-serj/bus-golang-publishers-registry-aws-servicediscovery"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
)

func TestClient_Watch(t *testing.T) {
	expected := []bus.PublisherEndpoint{
		"tcp://1.2.3.4:5678",
		"tcp://4.3.2.1:5555",
	}

	stubHTTPClient := smithyhttp.ClientDoFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.String() {
		case "https://servicediscovery.test-region.amazonaws.com/":
			reqBodyBytes, _ := io.ReadAll(req.Body)
			reqBody := string(reqBodyBytes)
			switch reqBody {
			case `{"Filters":[{"Condition":"EQ","Name":"NAMESPACE_ID","Values":["test-namespace"]}],"MaxResults":100}`:
				return &http.Response{
					StatusCode: 200,
					Header:     http.Header{},
					Body: io.NopCloser(strings.NewReader(`{
   "NextToken": "test-next-token",
   "Services": [ 
      { 
         "Arn": "test-arn-1",
         "CreateDate": 1679816197,
         "Description": "Test service 1",
         "DnsConfig": { 
            "DnsRecords": [ 
               { 
                  "TTL": 30,
                  "Type": "SRV"
               }
            ],
            "NamespaceId": "test-namespace"
         },
         "Id": "test-service-id-1",
         "InstanceCount": 1,
         "Name": "just-some-service"
      }
   ]
}`)),
				}, nil
			case `{"Filters":[{"Condition":"EQ","Name":"NAMESPACE_ID","Values":["test-namespace"]}],"MaxResults":100,"NextToken":"test-next-token"}`:
				return &http.Response{
					StatusCode: 200,
					Header:     http.Header{},
					Body: io.NopCloser(strings.NewReader(`{
   "NextToken": "test-next-token",
   "Services": [ 
      { 
         "Arn": "test-arn-2",
         "CreateDate": 1679816197,
         "Description": "Expected service",
         "DnsConfig": { 
            "DnsRecords": [ 
               { 
                  "TTL": 30,
                  "Type": "SRV"
               }
            ],
            "NamespaceId": "test-namespace"
         },
         "Id": "test-service-id-2",
         "InstanceCount": 2,
         "Name": "bus-test-event-name-v123"
      }
   ]
}`)),
				}, nil
			case `{"MaxResults":100,"ServiceId":"test-service-id-2"}`:
				return &http.Response{
					StatusCode: 200,
					Header:     http.Header{},
					Body: io.NopCloser(strings.NewReader(`{
   "Instances": [ 
      { 
         "Attributes": { 
            "AWS_INSTANCE_IPV4" : "1.2.3.4",
            "AWS_INSTANCE_PORT" : "5678"
         },
         "Id": "test-instance-1"
      }
   ],
   "NextToken": "test-next-token-for-list-services"
}`)),
				}, nil
			case `{"MaxResults":100,"NextToken":"test-next-token-for-list-services","ServiceId":"test-service-id-2"}`:
				return &http.Response{
					StatusCode: 200,
					Header:     http.Header{},
					Body: io.NopCloser(strings.NewReader(`{
   "Instances": [ 
      { 
         "Attributes": { 
            "AWS_INSTANCE_IPV4" : "4.3.2.1",
            "AWS_INSTANCE_PORT" : "5555"
         },
         "Id": "test-instance-2"
      }
   ]
}`)),
				}, nil
			}
		}

		panic("unexpected request")
	})

	serviceDiscoveryClient := servicediscovery.NewFromConfig(aws.Config{
		Region:       "test-region",
		DefaultsMode: aws.DefaultsModeStandard,
		HTTPClient:   stubHTTPClient,
	})

	resultChan := make(chan []bus.PublisherEndpoint)
	testable := registry.New(serviceDiscoveryClient, "test-namespace", func(err error) {
		panic("unexpected error")
	})
	stop, err := testable.Watch("test-event-name", 123, func(endpoints []bus.PublisherEndpoint) {
		resultChan <- endpoints
	})
	require.NoError(t, err)
	defer func() {
		_ = stop()
	}()

	select {
	case actual := <-resultChan:
		require.Equal(t, expected, actual)
	case <-time.After(5 * time.Second):
		t.Error("result waiting has been timed out")
	}
}
