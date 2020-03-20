package stored

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/brainfucker/zero"
)

// Cluster is the main  struct for handling work with fdb
type Cluster struct {
	db fdb.Database
}

// ClusterStatus is status command fdb format
type ClusterStatus struct {
	Client struct {
		ClusterFile struct {
			Path     string `json:"path"`
			UpToDate bool   `json:"up_to_date"`
		} `json:"cluster_file"`
		Coordinators struct {
			Coordinators []struct {
				Address   string `json:"address"`
				Reachable bool   `json:"reachable"`
			} `json:"coordinators"`
			QuorumReachable bool `json:"quorum_reachable"`
		} `json:"coordinators"`
		DatabaseStatus struct {
			Available bool `json:"available"`
			Healthy   bool `json:"healthy"`
		} `json:"database_status"`
		Messages []struct {
			Description string `json:"description"`
			Name        string `json:"name"`
		} `json:"messages"`
	} `json:"client"`
}

// Connect is main constructor for creating connections
func Connect(cluster string) *Cluster {
	fdb.MustAPIVersion(510)
	conn := Cluster{
		db: fdb.MustOpen(cluster, []byte("DB")),
	}
	return &conn
}

// Directory created an directury that could be used to work with stored
func (c *Cluster) Directory(name string) *Directory {
	subspace, err := directory.CreateOrOpen(c.db, []string{name}, nil)
	if err != nil {
		panic(err)
	}
	dir := Directory{
		Name:     name,
		Cluster:  c,
		Subspace: subspace,
	}
	dir.init()
	return &dir
}

// Status will return fdb cluster status
func (c *Cluster) Status() (*ClusterStatus, error) {
	status, err := c.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		keyCode := []byte("\xff\xff/status/json")
		var k fdb.Key
		k = keyCode
		resp, err := tr.Get(k).Get()
		if err != nil {
			return nil, err
		}
		if len(resp) == 0 {
			return nil, ErrNotFound
		}

		status := ClusterStatus{}
		err = json.Unmarshal(resp, &status)
		if err != nil {
			return nil, err
		}

		return &status, nil
	})
	if err != nil {
		return nil, err
	}
	return status.(*ClusterStatus), err
}

// Err return error if something wrong with cluster
func (c *Cluster) Err() error {
	status, err := c.Status()
	if err != nil {
		return err
	}
	msg := []string{}
	if !status.Client.DatabaseStatus.Available {
		msg = append(msg, "[Unavailable]")
	}
	if !status.Client.DatabaseStatus.Healthy {
		msg = append(msg, "[Unhealthy]")
		zero.LogJSON(status)
	}
	for _, errMessage := range status.Client.Messages {
		msg = append(msg, errMessage.Description)
	}
	if len(msg) == 0 {
		return nil
	}
	return errors.New(strings.Join(msg, " "))
}
