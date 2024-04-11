package dcgmexporter

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1alpha1"
)

func NewVGPUMapper(c *Config) (*VGPUMapper, error) {
	socketPath := c.PodResourcesKubeletSocket
	_, err := os.Stat(socketPath)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("No Kubelet socket, ignoring")
	}
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx,
		socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(
			grpc.ConnectParams{
				Backoff:           backoff.DefaultConfig,
				MinConnectTimeout: connectionTimeout,
			},
		),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			d := net.Dialer{}
			return d.DialContext(ctx, "unix", addr)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failure connecting to '%s'; err: %w", socketPath, err)
	}
	client := podresourcesapi.NewPodResourcesListerClient(conn)
	return &VGPUMapper{Config: c, Client: client}, nil
}

var _ Transform = &VGPUMapper{}

type VGPUMapper struct {
	Config *Config
	Client podresourcesapi.PodResourcesListerClient
}

// Name implements Transform.
func (v *VGPUMapper) Name() string {
	return "vgpuMapper"
}

// Process implements Transform.
func (v *VGPUMapper) Process(metrics MetricsByCounter, sysInfo SystemInfo) error {
	resp, err := v.Client.List(context.Background(), &podresourcesapi.ListPodResourcesRequest{})
	if err != nil {
		return fmt.Errorf("failure getting pod resources; err: %w", err)
	}
	vgpuToPod := v.toVGPUDeviceToPod(resp, sysInfo)
	for counter, list := range metrics {
		for _, val := range list {
			deviceID, err := val.getIDOfType(v.Config.KubernetesGPUIdType)
			if err != nil {
				return err
			}
			for vgpuid, podinfo := range vgpuToPod[deviceID] {
				newMeteics := val
				newMeteics.Attributes = map[string]string{
					podAttribute:       podinfo.Name,
					namespaceAttribute: podinfo.Namespace,
					containerAttribute: podinfo.Container,
					"VGPU_ID":          vgpuid,
				}
				list = append(list, newMeteics)
			}
		}
		metrics[counter] = list
	}
	return nil
}

func (v *VGPUMapper) toVGPUDeviceToPod(pods *podresourcesapi.ListPodResourcesResponse, _ SystemInfo) map[string]map[string]PodInfo {
	deviceToPodMap := make(map[string]map[string]PodInfo)
	for _, pod := range pods.GetPodResources() {
		for _, container := range pod.GetContainers() {
			for _, device := range container.GetDevices() {
				podInfo := PodInfo{
					Name:      pod.GetName(),
					Namespace: pod.GetNamespace(),
					Container: container.GetName(),
				}
				if device.GetResourceName() == "volcano.sh/vgpu-number" {
					for _, deviceID := range device.GetDeviceIds() {
						// "GPU-5a2fd470-109c-a456-9051-f4b09994f610-8"
						i := strings.LastIndex(deviceID, "-")
						if i <= 0 {
							continue
						}
						gpuid, vgpuindex := deviceID[:i], deviceID[i+1:]
						if gpuid == "" || vgpuindex == "" {
							continue
						}
						infos, ok := deviceToPodMap[gpuid]
						if !ok {
							infos = map[string]PodInfo{}
						}
						infos[vgpuindex] = podInfo
						deviceToPodMap[gpuid] = infos
					}
				}
			}
		}
	}
	return deviceToPodMap
}
