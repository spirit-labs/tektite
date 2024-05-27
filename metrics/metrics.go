package metrics

import (
	"errors"
	"github.com/spirit-labs/tektite/common"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spirit-labs/tektite/conf"
	log "github.com/spirit-labs/tektite/logger"
)

type (
	Labels        = prometheus.Labels
	Counter       = prometheus.Counter
	CounterVec    = prometheus.CounterVec
	CounterOpts   = prometheus.CounterOpts
	Gauge         = prometheus.Gauge
	GaugeVec      = prometheus.GaugeVec
	GaugeOpts     = prometheus.GaugeOpts
	SummaryOpts   = prometheus.SummaryOpts
	SummaryVec    = prometheus.SummaryVec
	HistogramOpts = prometheus.HistogramOpts
	HistogramVec  = prometheus.HistogramVec
	Observer      = prometheus.Observer
)

type Server struct {
	config     conf.Config
	httpServer *http.Server
	dummy      bool
}

type metricServer struct{}

func (ms *metricServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	promhttp.InstrumentMetricHandler(
		prometheus.DefaultRegisterer, promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{
			DisableCompression: true,
		}),
	).ServeHTTP(w, r)
}

func NewServer(config conf.Config, dummy bool) *Server {
	if dummy {
		return &Server{dummy: true}
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", &metricServer{})
	return &Server{
		config: config,
		httpServer: &http.Server{
			Addr:    config.MetricsBind,
			Handler: mux,
		},
	}
}

func (s *Server) Start() error {
	if s.dummy {
		return nil
	}
	common.Go(func() {
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Errorf("prometheus http export server failed to listen %v", err)
		} else {
			log.Debugf("Started prometheus http server on address %s", s.config.MetricsBind)
		}
	})
	return nil
}

func (s *Server) Stop() error {
	if s.dummy {
		return nil
	}
	if s.httpServer != nil {
		return s.httpServer.Close()
	}
	return nil
}
