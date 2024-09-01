package shutdown

import (
	"context"
	"fmt"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/asl/remoting"
	"github.com/spirit-labs/tektite/common"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"time"
)

func PerformShutdown(cfg *conf.Config, succeedIfNodeDown bool) error {
	log.Info("shutting down cluster")
	client := remoting.NewClient(cfg.ClusterTlsConfig)
	for {
		if err := doShutdown(cfg, client, succeedIfNodeDown); err != nil {
			log.Warnf("cluster shutdown failed with error %v - will retry after delay", err)
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}
	client.Stop()
	log.Info("cluster shutdown completed ok")
	return nil
}

func doShutdown(cfg *conf.Config, client *remoting.Client, succeedIfNodeDown bool) error {
	type phaseResponse struct {
		resp *clustermsgs.ShutdownResponse
		err  error
	}
	for phase := 1; phase <= 8; phase++ {
		flushed := false
		var chans []chan phaseResponse
		for _, address := range cfg.ClusterAddresses {
			ch := make(chan phaseResponse, 1)
			chans = append(chans, ch)
			theAddress := address
			thePhase := phase
			// Execute them in parallel for faster shutdown
			common.Go(func() {
				log.Debugf("Sending shutdown phase %d to address %s", thePhase, theAddress)
				start := time.Now()
				r, err := client.SendRPC(&clustermsgs.ShutdownMessage{Phase: uint32(thePhase)}, theAddress)
				if err != nil {
					if thePhase != 8 {
						// Normal to get EOF on phase 8
						log.Warnf("Sending shutdown phase %d for address %s returned with error %v",
							thePhase, theAddress, err)
					}
					ch <- phaseResponse{
						err: err,
					}
				} else {
					log.Infof("Shutdown phase %d for address %s took %d ms", thePhase, theAddress, time.Now().Sub(start).Milliseconds())
					resp := r.(*clustermsgs.ShutdownResponse)
					ch <- phaseResponse{
						resp: resp,
					}
				}
			})
		}
		for _, ch := range chans {
			pResp := <-ch
			if pResp.err != nil {
				var perr remoting.Error
				if errwrap.As(pResp.err, &perr) {
					if phase == 8 {
						// In the last phase we shut down the server which shuts down the remoting system - this is likely
						// to give a connection closed error, or similar. This is expected and we can ignore it.
						continue
					}
					if succeedIfNodeDown {
						// We also ignore if succeedIfNodeDown flag is true
						log.Warnf("Remoting error - will ignore %v", pResp.err)
						continue
					}
				}
				return pResp.err
			}
			if pResp.resp.Flushed {
				flushed = true
			}
		}
		if !flushed {
			if phase == 4 {
				// version manager must be flushed to ensure last flushed version written to level manager
				return errwrap.New("version-manager was not flushed")
			}
			if phase == 6 {
				// level manager must be flushed to ensure all level info incl last flushed version flushed to cloud
				return errwrap.New("level-manager was not flushed")
			}
		}
		log.Debugf("shutdown phase %d complete", phase)
	}

	// Now we delete the cluster from etcd
	clusterPrefix := fmt.Sprintf("%s%s/", cfg.ClusterManagerKeyPrefix, cfg.ClusterName)
	etcdLogger := log.CreateLogger(zap.ErrorLevel, "console")
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.ClusterManagerAddresses,
		DialTimeout: 5 * time.Second,
		Logger:      etcdLogger,
	})
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err = cli.Delete(ctx, clusterPrefix, clientv3.WithPrefix())
	}
	if err != nil {
		log.Warnf("failed to delete cluster state from etcd: %v", err)
	}
	if err := cli.Close(); err != nil {
		log.Debugf("failed to close etcd client: %v", err)
	}
	return nil
}
