package bus_golang_publishers_registry_aws_servicediscovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/al-kimmel-serj/bus"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
)

var (
	ErrServiceIsNotExist = errors.New("service is not exist")
)

type Client struct {
	namespaceID            string
	serviceDiscoveryClient *servicediscovery.Client
	errorHandler           func(error)
}

func New(serviceDiscoveryClient *servicediscovery.Client, namespaceID string, errorHandler func(error)) *Client {
	return &Client{
		namespaceID:            namespaceID,
		serviceDiscoveryClient: serviceDiscoveryClient,
		errorHandler:           errorHandler,
	}
}

func (c *Client) Register(_ bus.EventName, _ bus.EventVersion, _ string, _ int) (func() error, error) {
	// Register has not been supported for AWS ServiceDiscovery.
	// Use AWS::ServiceDiscovery::Service SRV in AWS CloudFormation template.
	return nil, nil
}

func (c *Client) Watch(eventName bus.EventName, eventVersion bus.EventVersion, handler func([]bus.PublisherEndpoint)) (func() error, error) {
	ctx, cancel := context.WithCancel(context.Background())

	serviceID, err := c.fetchServiceIDForEvent(ctx, eventName, eventVersion)
	if err != nil {
		cancel()
		return nil, err
	}

	ticker := time.NewTicker(30 * time.Second)
	go func() {
		for {
			endpoints, err := c.fetchEndpoints(ctx, serviceID, nil)
			if err != nil {
				if c.errorHandler != nil {
					c.errorHandler(err)
				}
			} else {
				handler(endpoints)
			}

			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-ticker.C:
			}
		}
	}()

	return func() error {
		cancel()
		return nil
	}, nil
}

func (c *Client) fetchServiceIDForEvent(ctx context.Context, eventName bus.EventName, eventVersion bus.EventVersion) (string, error) {
	var (
		maxResults        int32   = 100
		nextToken         *string = nil
		wantedServiceName         = fmt.Sprintf("bus-%s-v%d", eventName, eventVersion)
	)

	for {
		resp, err := c.serviceDiscoveryClient.ListServices(ctx, &servicediscovery.ListServicesInput{
			Filters: []types.ServiceFilter{
				{
					Name:      "NAMESPACE_ID",
					Values:    []string{c.namespaceID},
					Condition: "EQ",
				},
			},
			MaxResults: &maxResults,
			NextToken:  nextToken,
		})
		if err != nil {
			return "", err
		}

		for _, service := range resp.Services {
			if service.Name != nil && *service.Name == wantedServiceName && service.Id != nil {
				return *service.Id, nil
			}
		}

		nextToken = resp.NextToken
		if nextToken == nil {
			break
		}
	}

	return "", ErrServiceIsNotExist
}

func (c *Client) fetchEndpoints(ctx context.Context, serviceID string, nextToken *string) ([]bus.PublisherEndpoint, error) {
	var maxResults int32 = 100

	resp, err := c.serviceDiscoveryClient.ListInstances(ctx, &servicediscovery.ListInstancesInput{
		ServiceId:  &serviceID,
		MaxResults: &maxResults,
		NextToken:  nextToken,
	})
	if err != nil {
		return nil, err
	}

	endpoints := make([]bus.PublisherEndpoint, 0, len(resp.Instances))
	for _, instance := range resp.Instances {
		endpoints = append(endpoints, bus.PublisherEndpoint(fmt.Sprintf(
			"tcp://%s:%s",
			instance.Attributes["AWS_INSTANCE_IPV4"],
			instance.Attributes["AWS_INSTANCE_PORT"],
		)))
	}

	if resp.NextToken != nil {
		nextPagesOfEndpoints, err := c.fetchEndpoints(ctx, serviceID, resp.NextToken)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, nextPagesOfEndpoints...)
	}

	return endpoints, nil
}
