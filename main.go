package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"d7024e/src/kademlia"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, input io.Reader, output, diagnostics io.Writer) error {
	config := kademlia.DefaultConfig()
	flags := flag.NewFlagSet("kadlab", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	listen := flags.String("listen", "127.0.0.1:8000", "concrete IP:port, or auto:port to select a non-loopback IPv4 address")
	bootstrap := flags.String("bootstrap", "", "optional bootstrap IP:port or hostname:port")
	keyFile := flags.String("registry-keys", "", "JSON static DNS public keys and optional publisher seeds")
	keygen := flags.String("registry-keygen", "", "generate a registry key configuration for DOMAIN and exit")
	eventLog := flags.String("lookup-log", "", "write structured lookup events to a new JSONL file")
	headless := flags.Bool("headless", false, "serve until interrupted without reading stdin")
	timeout := flags.Duration("command-timeout", 30*time.Second, "deadline for joining and each shell command")
	flags.IntVar(&config.K, "k", config.K, "closest-node count")
	flags.IntVar(&config.Alpha, "alpha", config.Alpha, "lookup/write parallelism")
	flags.IntVar(&config.MaxValueSize, "max-value-size", config.MaxValueSize, "maximum value bytes")
	flags.DurationVar(&config.RPCTimeout, "rpc-timeout", config.RPCTimeout, "timeout per RPC attempt")
	flags.IntVar(&config.RPCRetries, "rpc-retries", config.RPCRetries, "extra RPC attempts")
	flags.DurationVar(&config.RefreshPeriod, "refresh-period", config.RefreshPeriod, "bucket refresh interval")
	flags.DurationVar(&config.ReplicationPeriod, "replication-period", config.ReplicationPeriod, "value replication interval")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if *timeout <= 0 {
		return fmt.Errorf("command-timeout must be positive")
	}
	if *keygen != "" {
		return generateRegistryKeys(*keygen, output)
	}
	owners, signingKeys, err := loadRegistryKeys(*keyFile)
	if err != nil {
		return err
	}
	config.Owners = owners
	ip, portText, err := net.SplitHostPort(*listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	if ip == "auto" {
		ip, err = localIPv4()
		if err != nil {
			return err
		}
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return fmt.Errorf("invalid listen port: %w", err)
	}
	if *eventLog != "" {
		file, err := os.OpenFile(*eventLog, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return fmt.Errorf("lookup log: %w", err)
		}
		defer file.Close()
		config.LookupLogger = kademlia.NewLookupLogger(file)
		defer func() {
			if err := config.LookupLogger.Err(); err != nil {
				fmt.Fprintf(diagnostics, "lookup log error: %v\n", err)
			}
		}()
	}
	transport, err := kademlia.Listen(ip, port)
	if err != nil {
		return err
	}
	node, err := kademlia.NewKademliaWithTransport(transport, config)
	if err != nil {
		_ = transport.Close()
		return err
	}
	defer node.Close()
	if *bootstrap != "" {
		joinCtx, cancel := context.WithTimeout(ctx, *timeout)
		address, err := resolvePeer(joinCtx, *bootstrap)
		if err == nil {
			err = node.Join(joinCtx, address)
		}
		cancel()
		if err != nil {
			return fmt.Errorf("join: %w", err)
		}
	}
	contact := node.Contact()
	fmt.Fprintf(output, "Ready %s id=%s\n", contact.Address, contact.ID.String())
	if *headless {
		<-ctx.Done()
		return nil
	}
	return runShell(ctx, node, config.MaxValueSize, *timeout, input, output, signingKeys)
}

func localIPv4() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, address := range addresses {
			ip, _, err := net.ParseCIDR(address.String())
			if err == nil && ip.To4() != nil && ip.IsGlobalUnicast() {
				return ip.String(), nil
			}
		}
	}
	return "", fmt.Errorf("no non-loopback IPv4 address; specify -listen explicitly")
}

func resolvePeer(ctx context.Context, address string) (string, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("peer port must be between 1 and 65535")
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return "", err
	}
	for _, ip := range ips {
		if ip.Zone == "" && !ip.IP.IsUnspecified() && !ip.IP.IsMulticast() {
			return net.JoinHostPort(ip.IP.String(), strconv.Itoa(port)), nil
		}
	}
	return "", fmt.Errorf("peer requires a concrete unicast address")
}
