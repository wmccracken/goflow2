package influxdb

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"strconv"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/jnovack/flag"
	"github.com/netsampler/goflow2/transport"
	log "github.com/sirupsen/logrus"
)

type InfluxDbDriver struct {
	client              influxdb2.Client
	writeApi            api.WriteAPI
	influxUrl           string
	influxToken         string
	influxOrganization  string
	influxBucket        string
	influxGZip          bool
	influxTLSSkipVerify bool
	influxLogErrors     bool
	q                   chan bool
}

func (d *InfluxDbDriver) Prepare() error {
	flag.StringVar(&d.influxUrl, "transport.influxdb.url", "http://localhost:8086", "InfluxDB URL including port")
	flag.StringVar(&d.influxToken, "transport.influxdb.token", "", "InfluxDB API token")
	flag.StringVar(&d.influxOrganization, "transport.influxdb.organization", "", "InfluxDB organization containing bucket")
	flag.StringVar(&d.influxBucket, "transport.influxdb.bucket", "", "InfluxDB bucket used for writing")
	flag.BoolVar(&d.influxGZip, "transport.influxdb.gzip", true, "Use GZip compression")
	flag.BoolVar(&d.influxTLSSkipVerify, "transport.influxdb.skiptlsverify", false, "Insecure TLS skip verify")
	flag.BoolVar(&d.influxGZip, "transport.influxdb.log.errors", true, "Log InfluxDB write errors")

	//TODO: Add retry parameters
	return nil
}

func (d *InfluxDbDriver) Init(context.Context) error {
	client := influxdb2.NewClientWithOptions(d.influxUrl, d.influxToken,
		influxdb2.DefaultOptions().
			SetUseGZip(d.influxGZip).
			SetTLSConfig(&tls.Config{
				InsecureSkipVerify: d.influxTLSSkipVerify,
			}))
	writeApi := client.WriteAPI(d.influxOrganization, d.influxBucket)
	d.client = client
	d.writeApi = writeApi
	d.q = make(chan bool)

	if d.influxLogErrors {
		errorCh := d.writeApi.Errors()

		go func() {
			for {
				select {
				case msg := <-errorCh:
					//if log != nil {
					log.Error(msg)
					//}
				case <-d.q:
					return
				}
			}
		}()
	}
	return nil
}

func (d *InfluxDbDriver) Send(key, data []byte) error {
	// convert json bytes back to FlowMessage
	var fmsg map[string]any
	err := json.Unmarshal(data, &fmsg)
	if err != nil {
		return err
	}

	p := influxdb2.NewPointWithMeasurement("flowdata").
		AddTag("type", fmsg["Type"].(string)).
		AddTag("sampler_address", fmsg["SamplerAddress"].(string)).
		AddTag("src_addr", fmsg["SrcAddr"].(string)).
		AddTag("dst_addr", fmsg["DstAddr"].(string)).
		AddTag("proto", strconv.FormatInt(int64(fmsg["proto"].(float64)), 10)).
		AddTag("src_port", strconv.FormatInt(int64(fmsg["SrcPort"].(float64)), 10)).
		AddTag("dst_port", strconv.FormatInt(int64(fmsg["DstPort"].(float64)), 10)).
		AddTag("in_if", strconv.FormatInt(int64(fmsg["InIf"].(float64)), 10)).
		AddTag("out_if", strconv.FormatInt(int64(fmsg["OutIf"].(float64)), 10)).
		AddField("time_received", int64(fmsg["TimeReceived"].(float64))).
		AddField("sequence_num", int64(fmsg["SequenceNum"].(float64))).
		AddField("sampling_rate", int64(fmsg["SamplingRate"].(float64))).
		AddField("flow_direction", int64(fmsg["FlowDirection"].(float64))).
		AddField("time_flow_start", int64(fmsg["TimeFlowStart"].(float64))).
		AddField("time_flow_end", int64(fmsg["TimeFlowEnd"].(float64))).
		AddField("time_flow_start_ms", int64(fmsg["TimeFlowStartMs"].(float64))).
		AddField("time_flow_end_ms", int64(fmsg["TimeFlowEndMs"].(float64))).
		AddField("bytes", int64(fmsg["Bytes"].(float64))).
		AddField("packets", int64(fmsg["Packets"].(float64))).
		AddField("etype", int64(fmsg["Etype"].(float64))).
		AddField("src_mac", fmsg["SrcMac"].(string)).
		AddField("dst_mac", fmsg["DstMac"].(string)).
		AddField("src_vlan", int64(fmsg["SrcVlan"].(float64))).
		AddField("dst_vlan", int64(fmsg["DstVlan"].(float64))).
		AddField("vlan_id", int64(fmsg["VlanId"].(float64))).
		AddField("ingress_vrf_id", int64(fmsg["IngressVrfId"].(float64))).
		AddField("egress_vrf_id", int64(fmsg["EgressVrfId"].(float64))).
		AddField("ip_tos", int64(fmsg["IpTos"].(float64))).
		AddField("forwarding_status", int64(fmsg["ForwardingStatus"].(float64))).
		AddField("ip_ttl", int64(fmsg["IpTtl"].(float64))).
		AddField("tcp_flags", int64(fmsg["TcpFlags"].(float64))).
		AddField("icmp_type", int64(fmsg["IcmpType"].(float64))).
		AddField("icmp_code", int64(fmsg["IcmpCode"].(float64))).
		AddField("ipv6_flow_label", int64(fmsg["Ipv6FlowLabel"].(float64))).
		AddField("fragment_id", int64(fmsg["FragmentId"].(float64))).
		AddField("fragment_offset", int64(fmsg["FragmentOffset"].(float64))).
		AddField("bi_flow_direction", int64(fmsg["BiFlowDirection"].(float64))).
		AddField("src_as", int64(fmsg["SrcAs"].(float64))).
		AddField("dst_as", int64(fmsg["DstAs"].(float64))).
		AddField("next_hop", fmsg["NextHop"].(string)).
		AddField("next_hop_as", int64(fmsg["NextHopAs"].(float64))).
		AddField("src_net", int64(fmsg["SrcNet"].(float64))).
		AddField("dst_net", int64(fmsg["DstNet"].(float64))).
		AddField("bgp_next_hop", fmsg["BgpNextHop"].([]interface{})).
		AddField("bgp_communities", fmsg["BgpCommunities"].([]interface{})).
		AddField("as_path", fmsg["AsPath"].([]interface{})).
		AddField("has_mpls", fmsg["HasMpls"].(bool)).
		AddField("mpls_count", int64(fmsg["MplsCount"].(float64))).
		AddField("mpls_1_ttl", int64(fmsg["Mpls_1Ttl"].(float64))).
		AddField("mpls_1_label", int64(fmsg["Mpls_1Label"].(float64))).
		AddField("mpls_2_ttl", int64(fmsg["Mpls_2Ttl"].(float64))).
		AddField("mpls_2_label", int64(fmsg["Mpls_2Label"].(float64))).
		AddField("mpls_3_ttl", int64(fmsg["Mpls_3Ttl"].(float64))).
		AddField("mpls_3_label", int64(fmsg["Mpls_3Label"].(float64))).
		AddField("mpls_last_ttl", int64(fmsg["MplsLastTtl"].(float64))).
		AddField("mpls_last_label", int64(fmsg["MplsLastLabel"].(float64))).
		AddField("mpls_label_ip", fmsg["MplsLabelIp"].([]interface{})).
		AddField("observation_domain_id", int64(fmsg["ObservationDomainId"].(float64))).
		AddField("observation_point_id", int64(fmsg["ObservationPointId"].(float64))).
		//AddField("custom_int_1", int64(fmsg["CustomInteger_1"].(float64))).
		//AddField("custom_int_2", int64(fmsg["CustomInteger_2"].(float64))).
		//AddField("custom_int_3", int64(fmsg["CustomInteger_3"].(float64))).
		//AddField("custom_int_4", int64(fmsg["CustomInteger_4"].(float64))).
		//AddField("custom_int_5", int64(fmsg["CustomInteger_5"].(float64))).
		//AddField("custom_bytes_1", fmsg["CustomBytes_1"].([]interface{})).
		//AddField("custom_bytes_2", fmsg["CustomBytes_2"].([]interface{})).
		//AddField("custom_bytes_3", fmsg["CustomBytes_3"].([]interface{})).
		//AddField("custom_bytes_4", fmsg["CustomBytes_4"].([]interface{})).
		//AddField("custom_bytes_5", fmsg["CustomBytes_5"].([]interface{})).
		SetTime(time.Unix(int64(fmsg["TimeReceived"].(float64)), 0))

	if val, ok := fmsg["SrcCountry"]; ok {
		p.AddField("src_country", val.(string))
	}
	if val, ok := fmsg["DstCountry"]; ok {
		p.AddField("dst_country", val.(string))
	}
	// write asynchronously
	d.writeApi.WritePoint(p)

	d.writeApi.Flush()
	return nil
}

func (d *InfluxDbDriver) Close(context.Context) error {
	d.client.Close()
	close(d.q)
	return nil
}

func init() {
	d := &InfluxDbDriver{}
	transport.RegisterTransportDriver("influxdb", d)
}
