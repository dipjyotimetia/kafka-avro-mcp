package main

import "testing"

func TestKafkaSecurity(t *testing.T) {
	env := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}

	tlsConfig, mechanism, err := kafkaSecurity(env(nil))
	if err != nil || tlsConfig != nil || mechanism != nil {
		t.Fatalf("unset environment = %v, %v, %v; want plaintext and no SASL", tlsConfig, mechanism, err)
	}

	tlsConfig, mechanism, err = kafkaSecurity(env(map[string]string{
		"KAFKA_TLS": "true", "KAFKA_SASL_MECHANISM": "scram-sha-512", "KAFKA_SASL_USER": "u", "KAFKA_SASL_PASSWORD": "p",
	}))
	if err != nil || tlsConfig == nil || mechanism == nil || mechanism.Name() != "SCRAM-SHA-512" {
		t.Fatalf("TLS with SCRAM = %v, %v, %v", tlsConfig, mechanism, err)
	}

	for name, values := range map[string]map[string]string{
		"unparseable TLS flag":  {"KAFKA_TLS": "sometimes"},
		"mechanism without key": {"KAFKA_SASL_MECHANISM": "PLAIN", "KAFKA_SASL_USER": "u"},
		"unknown mechanism":     {"KAFKA_SASL_MECHANISM": "GSSAPI", "KAFKA_SASL_USER": "u", "KAFKA_SASL_PASSWORD": "p"},
	} {
		if _, _, err := kafkaSecurity(env(values)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
