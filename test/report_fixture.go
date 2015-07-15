package test

import (
	"github.com/weaveworks/scope/probe/docker"
	"github.com/weaveworks/scope/report"
)

// This is an example Report:
//   2 hosts with probes installed - client & server.
var (
	ClientHostID  = "client.hostname.com"
	ServerHostID  = "server.hostname.com"
	UnknownHostID = ""

	ClientIP         = "10.10.10.20"
	ServerIP         = "192.168.1.1"
	ClientPort54001  = "54001"
	ClientPort54010  = "54010"
	ClientPort54002  = "54002"
	ClientPort54020  = "54020"
	ClientPort12345  = "12345"
	ServerPort       = "80"
	UnknownClient1IP = "10.10.10.10"
	UnknownClient2IP = "10.10.10.10"
	UnknownClient3IP = "10.10.10.11"
	RandomClientIP   = "51.52.53.54"

	ClientHostName = ClientHostID
	ServerHostName = ServerHostID

	Client1PID      = "10001"
	Client2PID      = "30020"
	ServerPID       = "215"
	NonContainerPID = "1234"

	ClientHostNodeID = report.MakeHostNodeID(ClientHostID)
	ServerHostNodeID = report.MakeHostNodeID(ServerHostID)

	Client54001NodeID    = report.MakeEndpointNodeID(ClientHostID, ClientIP, ClientPort54001) // curl (1)
	Client54002NodeID    = report.MakeEndpointNodeID(ClientHostID, ClientIP, ClientPort54002) // curl (2)
	Server80NodeID       = report.MakeEndpointNodeID(ServerHostID, ServerIP, ServerPort)      // apache
	UnknownClient1NodeID = report.MakeEndpointNodeID(ServerHostID, UnknownClient1IP, "54010") // we want to ensure two unknown clients, connnected
	UnknownClient2NodeID = report.MakeEndpointNodeID(ServerHostID, UnknownClient2IP, "54020") // to the same server, are deduped.
	UnknownClient3NodeID = report.MakeEndpointNodeID(ServerHostID, UnknownClient3IP, "54020") // Check this one isn't deduped
	RandomClientNodeID   = report.MakeEndpointNodeID(ServerHostID, RandomClientIP, "12345")   // this should become an internet node

	ClientProcess1NodeID      = report.MakeProcessNodeID(ClientHostID, Client1PID)
	ClientProcess2NodeID      = report.MakeProcessNodeID(ClientHostID, Client2PID)
	ServerProcessNodeID       = report.MakeProcessNodeID(ServerHostID, ServerPID)
	NonContainerProcessNodeID = report.MakeProcessNodeID(ServerHostID, NonContainerPID)

	ClientContainerID     = "a1b2c3d4e5"
	ServerContainerID     = "5e4d3c2b1a"
	ClientContainerNodeID = report.MakeContainerNodeID(ClientHostID, ClientContainerID)
	ServerContainerNodeID = report.MakeContainerNodeID(ServerHostID, ServerContainerID)

	ClientContainerImageID     = "imageid123"
	ServerContainerImageID     = "imageid456"
	ClientContainerImageNodeID = report.MakeContainerNodeID(ClientHostID, ClientContainerImageID)
	ServerContainerImageNodeID = report.MakeContainerNodeID(ServerHostID, ServerContainerImageID)
	ClientContainerImageName   = "image/client"
	ServerContainerImageName   = "image/server"

	ClientAddressNodeID   = report.MakeAddressNodeID(ClientHostID, "10.10.10.20")
	ServerAddressNodeID   = report.MakeAddressNodeID(ServerHostID, "192.168.1.1")
	UnknownAddress1NodeID = report.MakeAddressNodeID(ServerHostID, "10.10.10.10")
	UnknownAddress2NodeID = report.MakeAddressNodeID(ServerHostID, "10.10.10.11")
	RandomAddressNodeID   = report.MakeAddressNodeID(ServerHostID, "51.52.53.54") // this should become an internet node

	Report = report.Report{
		Endpoint: report.Topology{
			Adjacency: report.Adjacency{
				report.MakeAdjacencyID(Client54001NodeID): report.MakeIDList(Server80NodeID),
				report.MakeAdjacencyID(Client54002NodeID): report.MakeIDList(Server80NodeID),
				report.MakeAdjacencyID(Server80NodeID): report.MakeIDList(
					Client54001NodeID, Client54002NodeID, UnknownClient1NodeID, UnknownClient2NodeID,
					UnknownClient3NodeID, RandomClientNodeID),
			},
			NodeMetadatas: report.NodeMetadatas{
				// NodeMetadata is arbitrary. We're free to put only precisely what we
				// care to test into the fixture. Just be sure to include the bits
				// that the mapping funcs extract :)
				Client54001NodeID: report.NewNodeMetadata(report.Metadata{
					"addr":            ClientIP,
					"port":            ClientPort54001,
					"pid":             Client1PID,
					report.HostNodeID: ClientHostNodeID,
				}),
				Client54002NodeID: report.NewNodeMetadata(report.Metadata{
					"addr":            ClientIP,
					"port":            ClientPort54002,
					"pid":             Client2PID,
					report.HostNodeID: ClientHostNodeID,
				}),
				Server80NodeID: report.NewNodeMetadata(report.Metadata{
					"addr":            ServerIP,
					"port":            ServerPort,
					"pid":             ServerPID,
					report.HostNodeID: ServerHostNodeID,
				}),
			},
			EdgeMetadatas: report.EdgeMetadatas{
				report.MakeEdgeID(Client54001NodeID, Server80NodeID): report.EdgeMetadata{
					WithBytes:    true,
					BytesIngress: 100,
					BytesEgress:  10,
				},
				report.MakeEdgeID(Client54002NodeID, Server80NodeID): report.EdgeMetadata{
					WithBytes:    true,
					BytesIngress: 200,
					BytesEgress:  20,
				},

				report.MakeEdgeID(Server80NodeID, Client54001NodeID): report.EdgeMetadata{
					WithBytes:    true,
					BytesIngress: 10,
					BytesEgress:  100,
				},
				report.MakeEdgeID(Server80NodeID, Client54002NodeID): report.EdgeMetadata{
					WithBytes:    true,
					BytesIngress: 20,
					BytesEgress:  200,
				},
				report.MakeEdgeID(Server80NodeID, UnknownClient1NodeID): report.EdgeMetadata{
					WithBytes:    true,
					BytesIngress: 30,
					BytesEgress:  300,
				},
				report.MakeEdgeID(Server80NodeID, UnknownClient2NodeID): report.EdgeMetadata{
					WithBytes:    true,
					BytesIngress: 40,
					BytesEgress:  400,
				},
				report.MakeEdgeID(Server80NodeID, UnknownClient3NodeID): report.EdgeMetadata{
					WithBytes:    true,
					BytesIngress: 50,
					BytesEgress:  500,
				},
			},
		},
		Process: report.Topology{
			Adjacency: report.Adjacency{},
			NodeMetadatas: report.NodeMetadatas{
				ClientProcess1NodeID: report.NewNodeMetadata(report.Metadata{
					"pid":              Client1PID,
					"comm":             "curl",
					docker.ContainerID: ClientContainerID,
					report.HostNodeID:  ClientHostNodeID,
				}),
				ClientProcess2NodeID: report.NewNodeMetadata(report.Metadata{
					"pid":              Client2PID,
					"comm":             "curl",
					docker.ContainerID: ClientContainerID,
					report.HostNodeID:  ClientHostNodeID,
				}),
				ServerProcessNodeID: report.NewNodeMetadata(report.Metadata{
					"pid":              ServerPID,
					"comm":             "apache",
					docker.ContainerID: ServerContainerID,
					report.HostNodeID:  ServerHostNodeID,
				}),
				NonContainerProcessNodeID: report.NewNodeMetadata(report.Metadata{
					"pid":             NonContainerPID,
					"comm":            "bash",
					report.HostNodeID: ServerHostNodeID,
				}),
			},
			EdgeMetadatas: report.EdgeMetadatas{},
		},
		Container: report.Topology{
			NodeMetadatas: report.NodeMetadatas{
				ClientContainerNodeID: report.NewNodeMetadata(report.Metadata{
					docker.ContainerID:   ClientContainerID,
					docker.ContainerName: "client",
					docker.ImageID:       ClientContainerImageID,
					report.HostNodeID:    ClientHostNodeID,
				}),
				ServerContainerNodeID: report.NewNodeMetadata(report.Metadata{
					docker.ContainerID:   ServerContainerID,
					docker.ContainerName: "server",
					docker.ImageID:       ServerContainerImageID,
					report.HostNodeID:    ServerHostNodeID,
				}),
			},
		},
		ContainerImage: report.Topology{
			NodeMetadatas: report.NodeMetadatas{
				ClientContainerImageNodeID: report.NewNodeMetadata(report.Metadata{
					docker.ImageID:    ClientContainerImageID,
					docker.ImageName:  ClientContainerImageName,
					report.HostNodeID: ClientHostNodeID,
				}),
				ServerContainerImageNodeID: report.NewNodeMetadata(report.Metadata{
					docker.ImageID:    ServerContainerImageID,
					docker.ImageName:  ServerContainerImageName,
					report.HostNodeID: ServerHostNodeID,
				}),
			},
		},
		Address: report.Topology{
			Adjacency: report.Adjacency{
				report.MakeAdjacencyID(ClientAddressNodeID): report.MakeIDList(ServerAddressNodeID),
				report.MakeAdjacencyID(ServerAddressNodeID): report.MakeIDList(
					ClientAddressNodeID, UnknownAddress1NodeID, UnknownAddress2NodeID, RandomAddressNodeID), // no backlinks to unknown/random
			},
			NodeMetadatas: report.NodeMetadatas{
				ClientAddressNodeID: report.NewNodeMetadata(report.Metadata{
					"addr":            ClientIP,
					report.HostNodeID: ClientHostNodeID,
				}),
				ServerAddressNodeID: report.NewNodeMetadata(report.Metadata{
					"addr":            ServerIP,
					report.HostNodeID: ServerHostNodeID,
				}),
			},
			EdgeMetadatas: report.EdgeMetadatas{
				report.MakeEdgeID(ClientAddressNodeID, ServerAddressNodeID): report.EdgeMetadata{
					WithConnCountTCP: true,
					MaxConnCountTCP:  3,
				},
				report.MakeEdgeID(ServerAddressNodeID, ClientAddressNodeID): report.EdgeMetadata{
					WithConnCountTCP: true,
					MaxConnCountTCP:  3,
				},
			},
		},
		Host: report.Topology{
			Adjacency: report.Adjacency{},
			NodeMetadatas: report.NodeMetadatas{
				ClientHostNodeID: report.NewNodeMetadata(report.Metadata{
					"host_name":       ClientHostName,
					"local_networks":  "10.10.10.0/24",
					"os":              "Linux",
					"load":            "0.01 0.01 0.01",
					report.HostNodeID: ClientHostNodeID,
				}),
				ServerHostNodeID: report.NewNodeMetadata(report.Metadata{
					"host_name":       ServerHostName,
					"local_networks":  "10.10.10.0/24",
					"os":              "Linux",
					"load":            "0.01 0.01 0.01",
					report.HostNodeID: ServerHostNodeID,
				}),
			},
			EdgeMetadatas: report.EdgeMetadatas{},
		},
	}
)

func init() {
	if err := Report.Validate(); err != nil {
		panic(err)
	}
}
