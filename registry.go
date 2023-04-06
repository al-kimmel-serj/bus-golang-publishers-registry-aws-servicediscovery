package bus_golang_publishers_registry_aws_servicediscovery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/al-kimmel-serj/bus-golang"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
)

var (
	ErrServiceDoesNotExist = errors.New("service does not exist")
)

type Registry struct {
	namespaceID            string
	serviceDiscoveryClient *servicediscovery.Client
	errorHandler           func(error)
}

func New(
	serviceDiscoveryClient *servicediscovery.Client,
	namespaceID string,
	errorHandler func(error),
) *Registry {
	return &Registry{
		namespaceID:            namespaceID,
		serviceDiscoveryClient: serviceDiscoveryClient,
		errorHandler:           errorHandler,
	}
}

func (r *Registry) Register(eventName bus.EventName, eventVersion bus.EventVersion, ipV4 string, port int) (func() error, error) {
	ctx := context.Background()

	serviceID, err := r.fetchServiceIDForEvent(ctx, eventName, eventVersion)
	if err != nil {
		return nil, err
	}

	instanceID := buildInstanceID(ipV4, port)

	_, err = r.serviceDiscoveryClient.RegisterInstance(ctx, &servicediscovery.RegisterInstanceInput{
		Attributes: map[string]string{
			"AWS_INSTANCE_IPV4": ipV4,
			"AWS_INSTANCE_PORT": fmt.Sprintf("%d", port),
		},
		InstanceId: &instanceID,
		ServiceId:  &serviceID,
	})
	if err != nil {
		return nil, err
	}

	return func() error {
		_, err := r.serviceDiscoveryClient.DeregisterInstance(context.Background(), &servicediscovery.DeregisterInstanceInput{
			InstanceId: &instanceID,
			ServiceId:  &serviceID,
		})
		if err != nil {
			return err
		}

		return nil
	}, nil
}

func (r *Registry) Watch(eventName bus.EventName, eventVersion bus.EventVersion, handler func([]bus.PublisherEndpoint)) (func() error, error) {
	ctx, cancel := context.WithCancel(context.Background())

	serviceID, err := r.fetchServiceIDForEvent(ctx, eventName, eventVersion)
	if err != nil {
		cancel()
		return nil, err
	}

	ticker := time.NewTicker(30 * time.Second)
	go func() {
		for {
			endpoints, err := r.fetchEndpoints(ctx, serviceID, nil)
			if err != nil {
				if r.errorHandler != nil {
					r.errorHandler(err)
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

func (r *Registry) fetchServiceIDForEvent(ctx context.Context, eventName bus.EventName, eventVersion bus.EventVersion) (string, error) {
	var (
		maxResults        int32   = 100
		nextToken         *string = nil
		wantedServiceName         = buildServiceName(eventName, eventVersion)
	)

	for {
		resp, err := r.serviceDiscoveryClient.ListServices(ctx, &servicediscovery.ListServicesInput{
			Filters: []types.ServiceFilter{
				{
					Name:      "NAMESPACE_ID",
					Values:    []string{r.namespaceID},
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

	return "", ErrServiceDoesNotExist
}

func buildServiceName(eventName bus.EventName, eventVersion bus.EventVersion) string {
	return fmt.Sprintf("bus-%s-v%d", eventName, eventVersion)
}

func (r *Registry) fetchEndpoints(ctx context.Context, serviceID string, nextToken *string) ([]bus.PublisherEndpoint, error) {
	var maxResults int32 = 100

	resp, err := r.serviceDiscoveryClient.ListInstances(ctx, &servicediscovery.ListInstancesInput{
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
		nextPagesOfEndpoints, err := r.fetchEndpoints(ctx, serviceID, resp.NextToken)
		if err != nil {
			return nil, err
		}
		endpoints = append(endpoints, nextPagesOfEndpoints...)
	}

	return endpoints, nil
}

func buildInstanceID(ipV4 string, port int) string {
	return fmt.Sprintf("%s:%d", ipV4, port)
}
